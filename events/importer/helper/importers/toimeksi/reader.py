from typing import Iterable
import sys
import requests
import logging
from .record import Record

log = logging.getLogger(__name__)


class Reader:
    endpoint_url = 'https://www.toimeksi.fi/wp-json'
    rest_user_agent = 'HelsinkiVETImporter/0.1'
    timeout = 5.0
    keyword_reader = None

    def __init__(self):
        pass

    def _setup_client(self):
        headers = {
            'Accept': 'application/json',
            'User-Agent': self.rest_user_agent
        }

        s = requests.Session()
        s.headers.update(headers)

        return s

    def load_entry(self, id):
        return self._load_entry_api(id)

    def _load_entry_api(self, task_id):
        http_client = self._setup_client()
        url = "%s/wp/v2/tm_volunteer/%s" % (self.endpoint_url, task_id)
        response = http_client.get(url, timeout=self.timeout)
        if response.status_code != 200:
            raise RuntimeError("Failed to request data from Toimeksi API! HTTP/%d" %
                               response.status_code)

        data = response.json()
        if not data:
            return None

        data_obj = Record(data)

        return data_obj

    def load_entries(self):
        ret = list(self.iterate())
        total_records = len(ret)

        return total_records, ret

    def iterate(self, match_callback=None) -> Iterable[Record]:
        http_client = self._setup_client()
        page = 1
        total_pages = sys.maxsize
        total_records = None
        data = True
        while data and page <= total_pages:
            url = "%s/wp/v2/tm_volunteer?per_page=20&page=%d" % (self.endpoint_url, page)
            log.debug("API-request: %s" % url)
            response = http_client.get(url, timeout=self.timeout)

            if response.status_code != 200:
                raise RuntimeError("Failed to request data from Toimeksi API! HTTP/%d" %
                                   response.status_code)

            # Limits:
            if not total_records and 'X-WP-Total' in response.headers:
                total_records = int(response.headers['X-WP-Total'])
            if total_pages == sys.maxsize and 'X-WP-TotalPages' in response.headers:
                total_pages = int(response.headers['X-WP-TotalPages'])

            data = response.json()
            if data:
                from .keyword import KeywordReader
                if not Reader.keyword_reader:
                    Reader.keyword_reader = KeywordReader()
                for record in data:
                    # log.debug("Processing id: %d" % record['id'])
                    if match_callback:
                        if not match_callback(record):
                            # log.debug("Skipping non-matching record.")
                            continue
                    data_obj = Record(record, keyword_callback=Reader._keyword_helper)
                    yield data_obj

                page += 1

    @staticmethod
    def _keyword_helper(keyword_id):
        return Reader.keyword_reader.load_entry(keyword_id)

    def load_photo(self, id):
        raise NotImplementedError("No photos yet!")
