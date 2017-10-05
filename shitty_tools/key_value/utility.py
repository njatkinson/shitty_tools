from collections import MutableMapping, defaultdict
from thread import get_ident
from threading import Thread
from random import choice
from Queue import Queue
from zlib import adler32


class ReadOnlyDict(MutableMapping):
    def __init__(self, wrapped_dict):
        '''
        Wraps a dict to only allow reads and no writes or deletes. Writes and deletes return immediately.
        :param wrapped_dict:
        '''
        self.wrapped_dict = wrapped_dict
    def __getitem__(self, key):
        return self.wrapped_dict[key]
    def __iter__(self):
        for key in self.wrapped_dict:
            yield key
    def __len__(self):
        return len(self.wrapped_dict)
    def __setitem__(self, key, value):
        return
    def __delitem__(self, key):
        return


class WriteOnlyDict(MutableMapping):
    def __init__(self, wrapped_dict):
        '''
        Wraps a dict to only allow writes and no reads. Trying to access a key raises a KeyError.
        Len and iteration return 0 and empty respectively.
        :param wrapped_dict: dict
        '''
        self.wrapped_dict = wrapped_dict
    def __getitem__(self, key):
        raise KeyError
    def __iter__(self):
        for nothing in []: yield nothing
    def __len__(self):
        return 0
    def __setitem__(self, key, value):
        self.wrapped_dict[key] = value
    def __delitem__(self, key):
        del(self.wrapped_dict[key])


class SerializedDict(MutableMapping):
    def __init__(self, wrapped_dict, key_serialize = None, key_deserialize = None,
                 value_serialize = None, value_deserialize = None):
        '''
        Takes a dict and returns that dict wrapped with serializers for keys and values
        :param wrapped_dict: dict
        :param key_serialize: function that accepts a key and returns the key serialized to string
        :param key_deserialize: function that accepts a serialized key and returns it in applications expected format
        :param value_serialize: function that accepts a value and returns the value serialized to string
        :param value_deserialize: function that accepts a value and returns the deserialized version
        '''
        noop = lambda x: x
        self.wrapped_dict = wrapped_dict
        self.key_serialize = key_serialize or noop
        self.key_deserialize = key_deserialize or noop
        self.value_serialize = value_serialize or noop
        self.value_deserialize = value_deserialize or noop
    def __getitem__(self, key):
        return self.value_deserialize(self.wrapped_dict[self.key_serialize(key)])
    def __iter__(self):
        for key in self.wrapped_dict.keys():
            yield self.key_deserialize(key)
    def __len__(self):
        return len(self.wrapped_dict)
    def __setitem__(self, key, value):
        self.wrapped_dict[self.key_serialize(key)] = self.value_serialize(value)
    def __delitem__(self, key):
        del(self.wrapped_dict[self.key_serialize(key)])


class RandomChoiceDict(MutableMapping):
    def __init__(self, wrapped_dict_list):
        self._wrapped_dict_list = wrapped_dict_list
        # Needless optimization
        if len(self._wrapped_dict_list == 1):
            self._get_random_dict = lambda: self._wrapped_dict_list[0]
        self._get_random_dict = lambda: choice(self._wrapped_dict_list)
    def __getitem__(self, key):
        return self._get_random_dict()[key]
    def __iter__(self):
        for key in self._get_random_dict().keys():
            yield key
    def __len__(self):
        return len(self._get_random_dict())
    def __setitem__(self, key, value):
        self._get_random_dict()[key] = value
    def __delitem__(self, key):
        del(self._get_random_dict()[key])


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


class ShardedDict(MutableMapping):
    def __init__(self, storage_backends):
        '''
        :param storage_backends: An indexable iterator that contains a list of storage backends with dictionary
         interfaces. Items will be stored/retrieved as follows: storage_backends[adler32(key) % len(storage_backends)]
        '''
        self.storage_backends = storage_backends
    def __getitem__(self, key):
        return self.storage_backends[adler32(key) % len(self.storage_backends)][key]
    def __setitem__(self, key, value):
        self.storage_backends[adler32(key) % len(self.storage_backends)][key] = value
    def __delitem__(self, key):
        del(self.storage_backends[adler32(key) % len(self.storage_backends)][key])
    def __iter__(self):
        for i, storage_backend in enumerate(self.storage_backends):
            for key in storage_backend:
                if adler32(key) % len(self.storage_backends) == i:
                    yield key
    def __len__(self):
        def backend_len((index, backend)):
            return len([key for key in backend if adler32(key) % len(self.storage_backends) == index])
        return sum(map(backend_len, enumerate(self.storage_backends)))


class ThreadedSerialAccessDict(MutableMapping):
    # TODO: Finish
    def __init__(self, wrapped_dict):
        self.operation_queue = Queue()
        self.response_queue_dict = defaultdict(Queue)
        self.wrapped_dict = wrapped_dict

        def operate():
            def _get(op, key, thread_id):
                self.response_queue_dict[thread_id].put(wrapped_dict[key])
            def _set(op, key, value):
                wrapped_dict[key] = value
            def _del(op, key):
                del(wrapped_dict[key])
            def _iter(op, thread_id):
                self.response_queue_dict[thread_id].put(wrapped_dict.keys())
            def _len(op, thread_id):
                self.response_queue_dict[thread_id].put(len(wrapped_dict))
            op_f_dict = {'get': _get,
                         'set': _set,
                         'del': _del,
                         'iter': _iter,
                         'len': _len
                         }
            while True:
                message = self.operation_queue.get()
                op_f_dict[message['op']](**message)
                self.operation_queue.task_done()

        self.operation_thread = Thread(target=operate)
        self.operation_thread.daemon = True
        self.operation_thread.start()


    def __getitem__(self, key):
        thread_id = get_ident()
        response_queue = self.response_queue_dict[thread_id]
        self.operation_queue.put({'op': 'get', 'key': key, 'thread_id': thread_id})
        return response_queue.get()
    def __setitem__(self, key, value):
        self.operation_queue.put({'op': 'set', 'key': key, 'value': value})
    def __delitem__(self, key):
        self.operation_queue.put({'op': 'del', 'key': key})
    def __iter__(self):
        thread_id = get_ident()
        response_queue = self.response_queue_dict[thread_id]
        self.operation_queue.put({'op': 'iter', 'thread_id': thread_id})
        for key in response_queue.get():
            yield key
    def __len__(self):
        thread_id = get_ident()
        response_queue = self.response_queue_dict[thread_id]
        self.operation_queue.put({'op': 'len', 'thread_id': thread_id})
        return response_queue.get()