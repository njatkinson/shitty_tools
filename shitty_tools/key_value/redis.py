from collections import MutableMapping


class RedisDict(MutableMapping):
    def __init__(self, redis_conn, key_prefix = '', exp_time = 0, sliding_expiry = False,
                 serializer = lambda x: x, deserializer = lambda x: x):
        '''
        Provides a dictionary interface to a Redis k-v store.

        >>> from shitty_tools.key_value.redis import RedisDict
        >>> from redis import Redis
        >>> r = Redis('localhost')
        >>> rkv = RedisDict(r)
        >>> r['foo'] = 'bar'
        >>> r['foo']
        'bar'

        :param redis_conn: a Redis object
        :param key_prefix: (transparently) add a prefix to all keys when storing in the backend
        :param exp_time: expiration time to use when setting values
        :param sliding_expiry: extend the expiration time of values when reading them
        :param serializer:
        :param deserializer:
        '''
        self.redis = redis_conn
        self.key_prefix = key_prefix
        self.exp_time = exp_time
        self.sliding_expiry = sliding_expiry
        self.serializer = serializer
        self.deserializer = deserializer


    def __getitem__(self, key):
        complete_key = self.key_prefix + key
        value = self.redis.get(complete_key)
        if value is None:
            raise KeyError
        try:
            if self.sliding_expiry and self.exp_time:
                self.redis.expire(complete_key, self.exp_time)
        except Exception:
            pass
        return self.deserializer(value)


    def __setitem__(self, key, value):
        complete_key = self.key_prefix + key
        serialized_value = self.serializer(value)
        if self.exp_time:
            self.redis.setex(complete_key, serialized_value, self.exp_time)
        else:
            self.redis.set(complete_key, serialized_value)


    def __delitem__(self, key):
        complete_key = self.key_prefix + key
        self.redis.delete(complete_key)


    def __iter__(self):
        for key in self.redis.keys():
            # TODO: Better way to get keys that start with a prefix?
            if key.startswith(self.key_prefix):
                yield key.lstrip(self.key_prefix)


    def __len__(self):
        return len(self.redis.keys())