from datetime import datetime, timedelta
import pytz
from dateutil.tz import tzlocal
import logging

logger = logging.getLogger(__name__)


class Record:
    STATUS_PUBLISHED = 'publish'
    STATUS = [STATUS_PUBLISHED]

    LOCALE_FI = 'fi'
    LOCALE_SE = 'se'
    LOCALE_EN = 'en'
    LOCALE = [LOCALE_FI, LOCALE_SE, LOCALE_EN]

    def __init__(self, json_dict, keyword_callback=None):
        self.id = json_dict['id']
        self.organization_name = json_dict['organization']
        self.title = json_dict['title']['rendered']
        # Alternate link: https://www.toimeksi.fi/?post_type=tm_volunteer&p= + self.id
        self.link = json_dict['link']
        if 'volunteer_location' in json_dict['acf'] and json_dict['acf']['volunteer_location'] and len(
                json_dict['acf']['volunteer_location']) and json_dict['acf']['volunteer_location'][0][
            'volunteer_location_map']:
            # Use the first location from the list. Typically there is only one to pick from.
            self.address = json_dict['acf']['volunteer_location'][0]['volunteer_location_map']['address']
            self.address_coordinates = {
                'lat': json_dict['acf']['volunteer_location'][0]['volunteer_location_map']['lat'],
                'lon': json_dict['acf']['volunteer_location'][0]['volunteer_location_map']['lng']
            }
        else:
            self.address = None
            self.address_coordinates = None
        if 'volunteer_location_online' in json_dict['acf'] and json_dict['acf']['volunteer_location_online']:
            self.location_online = True
        else:
            self.location_online = False
        if 'volunteer_location_phone' in json_dict['acf'] and json_dict['acf']['volunteer_location_phone']:
            self.location_phone = True
        else:
            self.location_phone = False
        self.tags = []
        for keyword in json_dict['tm_keyword']:
            theme_id = keyword
            if keyword_callback:
                theme_name = keyword_callback(theme_id)
            else:
                theme_name = None
            self.tags.append({theme_id: theme_name})

        if json_dict['acf']['volunteer_start_date']:
            self.timestamp_start = datetime.strptime(
                json_dict['acf']['volunteer_start_date'], "%d.%m.%Y").replace(
                tzinfo=tzlocal())
        else:
            # No start time at all. Use latest modify.
            self.timestamp_start = datetime.strptime(
                json_dict['modified_gmt'], '%Y-%m-%dT%H:%M:%S').replace(
                tzinfo=pytz.UTC)
        if json_dict['acf']['volunteer_end_date']:
            end_date = json_dict['acf']['volunteer_end_date']
        elif json_dict['acf']['volunteer_expires']:
            # No end time, using expiry
            end_date = json_dict['acf']['volunteer_expires']
        else:
            # No end time at all. Take current day and add two years.
            end_date = datetime.now().date() + timedelta(days=2 * 365)
            end_date = end_date.strftime("%d.%m.%Y")
        self.timestamp_end = datetime.strptime(
            end_date, "%d.%m.%Y").replace(
            tzinfo=tzlocal())
        self.description = json_dict['content']['rendered']
        if json_dict['status'] in self.STATUS:
            self.status = json_dict['status']
        else:
            raise RuntimeError("Unknown status %d!" % json_dict['status'])
        self.creator_id = json_dict['author']

        self.timestamp_inserted = datetime.strptime(
            json_dict['date_gmt'], '%Y-%m-%dT%H:%M:%S').replace(
            tzinfo=pytz.UTC)
        self.timestamp_updated = datetime.strptime(
            json_dict['modified_gmt'], '%Y-%m-%dT%H:%M:%S').replace(
            tzinfo=pytz.UTC)
