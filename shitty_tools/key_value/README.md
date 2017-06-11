# Key-Value Stores

All Key-Value stores in the module present essentially a dictionary-like
interface. You should be able to use them just like dictionaries.

They are thread safe unless otherwise noted.

## Notes

### File System

Offers the FileSystemDict object for interacting with the file system as
if it were a dictionary. It requires a storage path and a scratch path.
All data is written into the scratch path, and then renamed to the
appropriate key in the storage path. On POSIX compliant operating
systems file renames are atomic operations. Renames are not atomic on
Windows, so probably not a good idea to use this with multiple writers
on Windows.

The storage item supports `/`'s in keys, and will create appropriate
subdirectories as needed.

Values are expected to be strings/bytes. If they are not, supply
a serializer and a deserializer at object instantiation time.

Restrictions:

* The scatch directory cannot be a subdirectory of the storage path.
* On POSIX systems, the scratch directory and storage path must be part
of the same filesystem (required for atomic write support).
* `/`'s in keys are **file system directories**. Do not use them in key
names unless you specifically want to use directories in the underlying
storage. If you don't want to split files out into directories, don't
use `/`'s in your keys.
* Keys may not start with `/` as it is viewed as an attempt at directory
 traversal and will result in an exception.
* Keys like `../some_file` are attempts at directory traversal and will
result in an exception.
* Doing something like this `d['foo'] = 'bar'; d['foo/bar'] = 'foobar'`
will result in an exception as `foo` cannot be both a file and a
directory.


### Redis

Offers the RedisDict object for interacting with a Redis database as if
it were a dictionary. It requires a redis object to be passed in at
instantiation.

You may specify a key prefix to help keep your application keys separate
from other keys in the Redis database. For example, if you set the key
prefix to 'myapp:', on the client side nothing changes, you still access
values by `redis_dict_object['some_key']` but on the server they are
stored as `myapp:some_key`. When retrieving a list of keys from the
server, the RedisDict instance will filter out any keys that do not
start with the key prefix.

The RedisDict object offers three expiration models--
* Default-- no expiration. Does not set an expiration time when storing
items.
* Simple expiration-- when storing items (insert or update), they have a
life span of `exp_time` in seconds.
* Sliding expiration-- when storing items, they have a life span of
`exp_time` in seconds. Each time the item is retrieved, the time to live
for the object is extended by `exp_time`.


### Tiered Storage

Offers the TieredStorageDict for stacking dictionary-like key-value
storage objects.

At instantiation, it accepts a list of key-value
store instances.

Attempts to read start at the head of the storage
backends list and iterate towards the tail of the list until either the
item is found or the end of the list is reached, at which time a
KeyError will be thrown. Writes to tiered storage will begin writing at
the tail of the storage backend list and iterate towards the front of
the list.