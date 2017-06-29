from collections import MutableMapping
try:
    from urlparse import urljoin
except ImportError:
    # Module was moved in Python3
    from urllib.parse import urljoin
import requests


class FlaskKvDict(MutableMapping):
    def __init__(self, kv_url, session_headers = {}, session_auth = None):
        self.session = requests.Session()
        self.session.headers.update(session_headers)
        if session_auth is not None:
            self.session.auth = session_auth
        self.key_join = lambda key: urljoin(kv_url, key)

    def __getitem__(self, key):
        response = self.session.get(self.key_join(key))
        if response.status_code == 404:
            raise KeyError
        elif response.status_code != 200:
            raise Exception('Expected status code 200 or 404. Received: %s' %response.status_code)
        return response.text

    def __setitem__(self, key, value):
        response = self.session.post(self.key_join(key), data=value)
        if response.status_code != 204:
            raise Exception('Expected status code 204. Received: %s' % response.status_code)

    def __delitem__(self, key):
        response = self.session.delete(self.key_join(key))
        if response.status_code != 204:
            raise Exception('Expected status code 204. Received: %s' % response.status_code)

    def __iter__(self):
        response = self.session.get(self.key_join(''))
        if response.status_code != 200:
            raise Exception('Expected status code 200. Received: %s' % response.status_code)
        for key in response.json():
            yield key

    def __len__(self):
        response = self.session.get(self.key_join(''))
        if response.status_code != 200:
            raise Exception('Expected status code 200. Received: %s' % response.status_code)
        return len(response.json())