from collections import MutableMapping


class TieredStorageDict(MutableMapping):
    def __init__(self, storage_backends):
        '''
        :param storage_backends: A sliceable iterator that contains a list of storage backends with dictionary
         interfaces. The beginning of the list is higher layers (caches), last item in the list is the canonical
         source of truth final layer of storage.
        :return:
        '''
        self.storage_backends = storage_backends
    def __getitem__(self, key):
        # Do getting from highest level to lowest
        def get(backends):
            if not backends:
                raise KeyError
            try:
                return backends[0][key]
            except KeyError:
                value = get(backends[1::])
                # put data in higher layer storage on cache miss
                backends[0][key] = value
                return value
        return get(self.storage_backends)
    def __setitem__(self, key, value):
        # Do saving from lowest level to highest
        for backend in self.storage_backends[::-1]:
            backend[key] = value
    def __delitem__(self, key):
        # Delete from top to bottom so we don't end up in a strange cache state on delete failure
        for backend in self.storage_backends:
            del(backend[key])
    def __iter__(self):
        # Only return keys from bottom layer of storage
        return iter(self.storage_backends[-1])
    def __len__(self):
        # Only get length from bottom layer
        return len(self.storage_backends[-1])