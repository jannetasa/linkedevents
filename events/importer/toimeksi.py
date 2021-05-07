# -*- coding: utf-8 -*-
import logging

import pytz
from datetime import datetime, timedelta
from django import db
from django.conf import settings
from django.contrib.gis.geos import Point
from django.contrib.gis.db.models.functions import Distance
from django.core.management import call_command
from django_orghierarchy.models import Organization
from django.utils import timezone as django_timezone
from pytz import timezone
import bleach
import base64

from events.importer.util import replace_location
from events.models import (
    DataSource,
    Event,
    Keyword,
    Place,
    License
)
from .sync import ModelSyncher
from .base import Importer, register_importer
from .util import clean_text
from events.importer.helper.importers import toimeksi

# Per module logger
logger = logging.getLogger(__name__)


@register_importer
class ToimeksiImporter(Importer):
    importer_id = 'toimeksi'
    name = 'toimeksi'
    supported_languages = ['fi', 'sv', 'en']
    ok_tags = ['u', 'b', 'h2', 'h3', 'em', 'ul', 'li', 'strong', 'br', 'p', 'a']

    VET_KEYWORD_ID = "p3050"
    MUNICIPALITIES = [
        1528,  # Helsinki
        119,  # Espoo
        1784,  # Vantaa
        1577,  # Kauniainen
    ]

    def setup(self):
        ds_args = dict(id=self.importer_id)
        defaults = dict(name='Toimeksi')
        self.data_source, _ = DataSource.objects.get_or_create(defaults=defaults, **ds_args)

        org_args = dict(origin_id='u021600', data_source=self.data_source)
        defaults = dict(name='Toimeksi')
        self.organization, _ = Organization.objects.get_or_create(defaults=defaults, **org_args)

        self.task_feed = toimeksi.Reader()

        try:
            self.event_only_license = License.objects.get(id='event_only')
        except License.DoesNotExist:
            self.event_only_license = None

    def pk_get(self, resource_name, res_id=None):
        logger.debug("pk_get(%s, %s)" % (resource_name, res_id))
        record = self.task_feed.load_entry(res_id)

        return record

    def delete_and_replace(self, obj):
        obj.deleted = True
        obj.save(update_fields=['deleted'])
        # we won't stand idly by and watch Toimeksi delete needed units willy-nilly without raising a ruckus!
        if obj.events.count() > 0:
            # try to replace by Toimeksi and, failing that, matko
            replaced = replace_location(replace=obj, by_source=self.importer_id)
            if not replaced:
                # matko location may indeed be deleted by an earlier iteration
                replaced = replace_location(replace=obj, by_source='matko', include_deleted=True)
            if not replaced:
                # matko location may never have been imported in the first place, do it now!
                call_command('event_import', 'matko', places=True, single=obj.name)
                replaced = replace_location(replace=obj, by_source='matko')
            if not replaced:
                logger.warning("Toimeksi deleted location %s (%s) with events."
                               "No unambiguous replacement was found. "
                               "Please look for a replacement location and save it in the replaced_by field. "
                               "Until then, events will stay mapped to the deleted location." %
                               (obj.id, str(obj)))
        return True

    def mark_deleted(self, obj):
        if obj.deleted:
            return False
        return self.delete_and_replace(obj)

    def check_deleted(self, obj):
        return obj.deleted

    def _import_event(self, event_obj: toimeksi.Record):
        event = dict(event_obj.__dict__)
        logger.debug("Task id %s" % event_obj.id)
        event['id'] = '%s:%s' % (self.data_source.id, event_obj.id)
        event['origin_id'] = event_obj.id
        event['data_source'] = self.data_source
        event['publisher'] = self.organization
        event['headline'] = {}
        event['description'] = {}

        title = bleach.clean(event_obj.title, tags=[], strip=True)
        # long description is html formatted, so we don't want plain text whitespaces
        title = clean_text(title, True)
        Importer._set_multiscript_field(title, event, event_obj.LOCALE, 'headline')

        desc = bleach.clean(event_obj.description, tags=self.ok_tags, strip=True)
        # long description is html formatted, so we don't want plain text whitespaces
        desc = clean_text(desc, True)
        Importer._set_multiscript_field(desc, event, event_obj.LOCALE, 'description')

        now = datetime.now(pytz.UTC)
        # Import only at most one month old events
        cut_off_date = now - timedelta(days=31)
        cut_off_date.replace(tzinfo=pytz.UTC)
        end_date = event_obj.timestamp_end.replace(tzinfo=pytz.UTC)
        if end_date < cut_off_date:
            logger.debug("Skipping task %s. Has ended %s" % (event_obj.id, end_date))
            return None

        event['start_time'] = event_obj.timestamp_start
        event['end_time'] = end_date

        # Note: In Toimeksi tasks do not contain language information
        lang = 'fi'
        event['info_url'] = {}
        event['external_links'] = {}
        event['info_url'][lang] = event_obj.link
        event['external_links'][lang] = {}

        event['images'] = None
        event['keywords'] = self._import_keywords(event_obj)
        event['location'] = self._import_location(event_obj)

        if not event['location']:
            # Skip events not located in Greater Helsinki area
            logger.debug("Task %s has no known location. Skipping." % event_obj.id)
            return None

        return event

    def _import_keywords(self, event_obj):
        event_keywords = []

        try:
            kw = Keyword.objects.get(id="yso:%s" % self.VET_KEYWORD_ID)
        except Keyword.DoesNotExist:
            kw = None
        if not kw:
            raise RuntimeError("Fata: Cannot import Toimeksi! Missing YSO:%s keyword." % self.VET_KEYWORD_ID)

        event_keywords.append(kw)
        for tag_dict in event_obj.tags:
            tag_id = list(tag_dict.keys())[0]
            if tag_id in self.KEYWORDS:
                keyword_value = self.KEYWORDS[tag_id]
                if not keyword_value:
                    logger.warning("Keyword id %d doesn't have mapping!" % tag_id)
                    continue
                if isinstance(keyword_value, str):
                    keyword_value = [keyword_value]
                for keyword in keyword_value:
                    yso_id = "yso:%s" % keyword
                    # logger.debug("Keyword query for: %s" % yso_id)
                    try:
                        kw = Keyword.objects.get(id=yso_id)
                    except Keyword.DoesNotExist:
                        logger.warning("Task %s has keyword %s, which maps into a non-existent %s" % (
                            event_obj.id, tag_id, yso_id))
                        kw = None
                    if kw:
                        event_keywords.append(kw)
        logger.debug("Task %s: Got keywords: %s" % (event_obj.id, ', '.join([o.id for o in event_keywords])))

        return event_keywords

    def _import_location(self, event_obj):
        # DEBUG: Logging of all queries
        # logging.getLogger('django.db.backends').setLevel(logging.DEBUG)
        # Note: Toimeksi will return "standard" WGS 84 latitude/longtitude.
        # Note 2: WGS 84 == EPSG:4326
        # Note 3: In events_place table data is stored as EPSG:3067 (aka. ETRS89 / TM35FIN(E,N))
        #         See: https://epsg.io/3067
        # Note 4: PostGIS will do automatic translation from WGS 84 into EPSG:3067.
        #         For manual EPSG translations, see: https://epsg.io/transform
        if not event_obj.address_coordinates:
            logger.debug("%s has no geographical address!" % event_obj.id)
            return None
        ref_location = Point(event_obj.address_coordinates['lon'],
                             event_obj.address_coordinates['lat'],
                             srid=4326)
        # Query for anything within 100 meters
        # Note: flake8 doesn't allow this to be formatted in a readable way :-(
        places = Place.objects.filter(
            position__dwithin=(ref_location, 100.0)).filter(
            data_source_id='osoite').annotate(
            distance=Distance(
                "position", ref_location)).order_by(
            "distance")[:3]
        if not places:
            logger.warning("Failed to find any locations for task id %s!" % event_obj.id)
            return False

        logger.debug("Got %d places, picking %s" % (len(places), places[0].id))
        # for obj in places:
        #    logger.debug("%s: %s, %f" % (obj.id, obj.name, obj.distance))

        return {'id': places[0].id}

    def import_events(self):
        logger.info("Importing Toimeksi events")

        qs = Event.objects.filter(end_time__gte=datetime.now(),
                                  data_source=self.importer_id, deleted=False)

        self.syncher = ModelSyncher(qs, lambda obj: obj.origin_id, delete_func=ToimeksiImporter._mark_deleted)

        mcb = self._setup_municipality_limiting(self.MUNICIPALITIES)
        event_cnt = 0
        valid_event_cnt = 0
        for event_obj in self.task_feed.iterate(match_callback=mcb):
            event_cnt += 1
            event = self._import_event(event_obj)
            if event:
                obj = self.save_event(event)
                self.syncher.mark(obj)
                valid_event_cnt += 1

        self.syncher.finish(force=self.options['force'])
        logger.info("%d events seen, %d processed" % (event_cnt, valid_event_cnt))

    @staticmethod
    def _mark_deleted(obj):
        if obj.deleted:
            return False
        obj.deleted = True
        obj.save(update_fields=['deleted'])

        return True

    def _municipality_matching_helper(self, data: dict):
        if 'tm_municipality' not in data:
            # No criteria to match
            return True

        # If this record's list of municipalities contains any of pre-defined list's municipalities.
        # Then take this record, otherwise don't.
        if not any(elem in data['tm_municipality'] for elem in self._municipality_limit):
            return False

        return True


    def _setup_municipality_limiting(self, limit_to_municipalities: list):
        if limit_to_municipalities:
            self._municipality_limit = limit_to_municipalities
            mcb = self._municipality_matching_helper
        else:
            self._municipality_limit = None
            mcb = None

        return mcb


    # Note:
    # In Toimeksi API, there are 6000+ keywords in existence.
    # This mapping covers tiny fraction of it.
    KEYWORDS = {
        5176: "p15627",  # pop-up-toiminta (5176)
        3309: "p2433",  # ikäihmiset (3309)
        4160: ["p12827", "p2108"],  # avustaminen tapahtumissa (4160)
        1317: ["p2108", "p13644"],  # tapahtumajärjestely (1317)
        1082: "p4354",  # lapset (1082)
        4445: ["p4354", "p11617"],  # lapset ja nuoret (4445)
        960: "p3846",  # hyväntekeväisyys (960)
        7699: "p4539",  # Hyväntekeväisyysjärjestö (7699)
        1318: "p2108",  # tapahtumat (1318)
        1411: "p4508",  # varainhankinta (1411)
        1412: "p4508",  # varainkeräys (1412)
        1114: "p6165",  # maahanmuuttajat (1114)
        7569: ["p5590", "p4354"],  # aikuiset ja lapset yhdessä (7569)
        7066: "p143",  # leiri (7066)
        3657: "p298",  # mentorointi (3657)
        1299: ["p8917", "p19552"],  # syrjäytymisen ehkäisy (1299)
        1351: "p3577",  # tukihenkilöt (1351)
        2909: ["p3577", "p6900"],  # tukihenkilötoiminta (2909)
        3336: "p26028",  # kaveritoiminta (3336)
        1061: "p372",  # kulttuuri (1061)
        5465: ["p419", "p9376"],  # joulumyyjäiset (5465)
        7721: "p8357",  # potilas (7721)
        1155: "p11617",  # nuoret (1155)
        1407: "p6939",  # vapaa-ajan toiminnat (1407)
        4204: "p10060",  # kehitysvammaisuus (4204)
        1144: "p6455",  # muotoilu (1144)
        1460: "p26028",  # ystävätoiminta (1460)
        4786: None,  # Huomioiva yhdessäolo (4786)
        7024: "p21435",  # kesäleirit (7024)
        9066: None,  # kivaayhdessä (9066)
        3982: "p318",  # leikki (3982)
        4918: "p14483",  # pelaaminen (4918)
        4896: "p4330",  # uiminen (4896)
        7672: "p3276",  # fritid (7672)
        9633: "p20390",  # frivillig (9633)
        7670: "p6418",  # funktionsnedsättning (7670)
        7671: "p14084",  # funktionsvariation (7671)
        9634: "p12878",  # kamratstöd (9634)
        7668: "p12878",  # stödperson (7668)
        7669: "p20879",  # vän (7669)
        3928: "p26028",  # vänverksamhet (3928)
        9065: ["p143", "p30185"],  # leiriohjaaja (9065)
        7767: None,  # Näe hyvä mussa (7767)
        4224: "p178",  # ohjaaminen (4224)
        7766: "p29074",  # positiivinen pedagogiikka (7766)
        5943: ["p27921", "p143"],  # viikonloppuleirit (5943)
        1368: "p9860",  # työnhaku (1368)
        8957: "p12297",  # apua mielenterveyteen (8957)
        898: "p6907",  # auttava netti (898)
        899: "p6907",  # auttava puhelin (899)
        7372: "p8955",  # ehkäisevä mielenterveystyö (7372)
        7223: "p6173",  # elämänkriisi (7223)
        925: "p8955",  # ennaltaehkäisevä toiminta (925)
        3471: "p135",  # jaksaminen (3471)
        6076: ["p135", "p9131"],  # jaksamisen tukeminen (6076)
        7748: "p11666",  # kriisipalvelu (7748)
        1057: "p6907",  # kriisipuhelimet (1057)
        1118: "p13777",  # maaseutu (1118)
        7455: "p13777",  # Maaseutu 2020 (7455)
        4837: "p3059",  # maaseutuyrittäjä (4837)
        7250: "p13245",  # Puhelintuki (7250)
        5159: "p3577",  # tukihenkilö (5159)
        9112: ["p3577", "p9270"],  # tukihenkilökurssi (9112)
        4781: "p3577",  # tukihenkilönä kahdenkeskisessä tukisuhteessa (4781)
        9106: "p130",  # työuupumus (9106)
        9105: "p130",  # uupumus (9105)
        8672: "p6935",  # vaivaako yksinäisyys (8672)
        2774: "p20390",  # vapaaehtoinen (2774)
        5819: ["p20390", "p12878"],  # vapaaehtoinen vertaisosaaja (5819)
        5818: "p27499",  # vapaaehtoistyö vertaistoiminta (5818)
        8402: ["p6624", "p12878"],  # verkkovertaistuki (8402)
        8983: "p12877",  # vertaisryhmiä ja tukea (8983)
        5002: "p12877",  # vertaisryhmä (5002)
        7591: ["p6624", "p12878"],  # vertaistuki verkossa (7591)
        7256: ["p12878", "p3577"],  # vertaistukihenkilö (7256)
        3624: "p17567",  # vertaisuus (3624)
        7519: ["p6935", "p19552"],  # yksinäisyyden lieventäminen (7519)
        8428: ["p1947", "p12732"],  # hyvinvoinnin ja terveyden edistäminen (8428)
        9079: "p1118",  # kouluttaja (9079)
        4482: "p20019",  # rintasyöpä (4482)
        9077: "p2762",  # rintaterveys (9077)
        4483: "p2762",  # rintojen omatarkkailu (4483)
        5158: "p20879",  # ystävä (5158)
        4628: ["p20405", "p5798"],  # digitaidot (4628)
        8559: ["p7560", "p20879"],  # juttukaveri (8559)
        8560: "p20879",  # kaveri (8560)
        4906: None,  # tutustuminen (4906)
        2725: "p10591",  # vuorovaikutus (2725)
        1439: "p17567",  # yhdenvertaisuus (1439)
        8689: "p7560",  # yhteydenpito (8689)
        1095: "p916",  # liikunta (1095)
        1207: "p8357",  # potilaat (1207)
        942: "p2901",  # harrastukset (942)
        3885: "p23102",  # liikkuminen (3885)
        3869: "p24074",  # kotoutuminen (3869)
        5184: ["p8856", "p38117"],  # Suomen kielen opetus (5184)
        3200: "p22129",  # vapaaehtoisryhmä (3200)
        5532: "p3594",  # hypistelymuhvi (5532)
        1077: "p485",  # käsityö (1077)
        5533: "p1377",  # suunnitteleminen (5533)
        4076: "p29963",  # elävä musiikki (4076)
        4071: "p419",  # joulu (4071)
        5397: None,  # kun sanat eivät riitä (5397)
        3128: "p485",  # käsityöt (3128)
        5396: "p8511",  # mielikuvitus (5396)
        4840: ["p2851", "p8311"],  # Taide ja luovuus (4840)
        4007: "p2901",  # harrastus (4007)
        3925: ["p2901", "p9131"],  # harrastusten tuki (3925)
        4191: "p3276",  # vapaa-aika (4191)
        4785: "p11494",  # varhainen vuorovaikutus (4785)
        872: ["p21733", "p2736"],  # aktiivinen kansalaisuus (872)
        1107: "p13084",  # luonto (1107)
        4431: ["p16991", "p7196"],  # nais-ja tyttöerityinen toiminta (4431)
        3096: "p2512",  # kirjoittaminen (3096)
        1293: "p17963",  # suru (1293)
        2806: ["p1393", "p36"],  # yhdistysten viestintä (2806)
        3819: "p6935",  # yksinäisyys (3819)
        1156: "p11617",  # nuoriso (1156)
        1298: "p8917",  # syrjäytyminen (1298)
    }
