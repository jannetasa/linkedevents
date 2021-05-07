from typing import Iterable

import requests
import logging
from .record import Record
from .reader import Reader

log = logging.getLogger(__name__)


class MunicipalityReader(Reader):

    def load_entry(self, id):
        return self._load_entry_api(id)

    def _load_entry_api(self, municipality_id: int):
        http_client = self._setup_client()
        url = "%s/wp/v2/tm_municipality/%d" % (self.endpoint_url, municipality_id)
        response = http_client.get(url, timeout=self.timeout)
        if response.status_code != 200:
            raise RuntimeError("Failed to request data from Toimeksi API! HTTP/%d" %
                               response.status_code)

        data = response.json()
        if not data:
            return None

        data_obj = {
            'id': data['id'],
            'name': data['name'],
        }

        return data_obj

    def load_entries(self):
        ret = list(self.iterate())
        total_records = len(ret)

        return total_records, ret

    def iterate(self) -> Iterable[dict]:
        http_client = self._setup_client()
        page = 1
        total_records = None
        data = True
        while data:
            url = "%s/wp/v2/tm_municipality?_fields=id,name&per_page=100&page=%d" % (self.endpoint_url, page)
            response = http_client.get(url, timeout=self.timeout)

            if response.status_code != 200:
                raise RuntimeError("Failed to request data from Toimeksi API! HTTP/%d" %
                                   response.status_code)
            if not total_records and 'X-WP-Total' in response.headers:
                total_records = int(response.headers['X-WP-Total'])
            data = response.json()
            if data:
                for record in data:
                    data_obj = {
                        'id': record['id'],
                        'name': record['name'],
                    }
                    yield data_obj

                page += 1

    def load_photo(self, id):
        raise NotImplementedError("Keywords don't have photos!")
