# Key-Value Stores

All Key-Value stores in the module present essentially a dictionary-like
interface. You should be able to use them just like dictionaries with the
caveat that all keys and values should be strings.

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


### Flask

Offers a Flask extension allowing you to expose a dict as REST API and a
client to allow remote access to the API in dictionary format via
FlaskKvDict.

Example server:
```
>>> from shitty_tools.key_value import flask_kv
>>> app = flask_kv.server.construct_kv_app(dict())
>>> app.run(port = 5000)
 * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
127.0.0.1 - - [29/Jun/2017 15:57:38] "POST /foo HTTP/1.1" 204 -
127.0.0.1 - - [29/Jun/2017 15:57:41] "GET / HTTP/1.1" 200 -
127.0.0.1 - - [29/Jun/2017 15:57:43] "GET / HTTP/1.1" 200 -
127.0.0.1 - - [29/Jun/2017 15:57:43] "GET /foo HTTP/1.1" 200 -
```

Example client:
```
>>> from shitty_tools.key_value import flask_kv
>>> client_dict = flask_kv.client.FlaskKvDict('http://127.0.0.1:5000')
>>> client_dict['foo'] = 'bar'
>>> client_dict.keys()
['foo']
>>> for k, v in client_dict.iteritems(): print k, v
...
foo bar
```

You can also use this with an existing application as a Flask extension.

Example extension:
```
>>> from flask import Flask
>>> from shitty_tools.key_value import flask_kv
>>> app = Flask(__name__)
>>> flask_kv.FlaskKv(app, dict(), url_prefix = '/kv')
<flask.blueprints.Blueprint object at 0x0344FED0>
>>> app.run()
 * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
```

Instantiation as a Flask extension returns the generated Flask blueprint
so that you can modify it to include authorization, logging, etc.


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


### SQL

Offers SqlDict objects for interacting with a table in an RDBMS supported
by sqlalchemy as if it were a dictionary. It requires a database connection
string and a table name.

The schema for the key-value table looks like this--

    Column('label', String(key_length), nullable=False),
    Column('sequence_number', BigInteger, primary_key=True),
    Column('item', LargeBinary),
    Column('is_deleted', Integer, nullable=False, default=False),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    Index('label_seq_uq', 'label', 'sequence_number', unique=True),
    Index('is_deleted_idx', 'is_deleted')

If the table name specified does not exist when you instantiate an object,
the object will try to create the table for you.

By default, the SqlDict maintains a history of all writes and returns the
most recently written value for any key (based on autoincrementing sequence
number). So, if you instantiate a SqlDict with a fresh table and run
`for i in range(1000): some_sql_dict['foo'] = 'bar'` you will now have 1000
records in the table. If you then `del(some_sql_dict['foo'])` you'll now
have 1001 records in the table.

There are a couple reasons for this behaviour.

* It made the code slightly easier to write.
* You want to reference records from the key-value table somewhere else
in the database? Well, your job just got a lot harder. If you wanted to
actually relational database, you shouldn't be using a tool that makes it
appear to be a key-value store. You're welcome. If you insist on doing
this, just use the memcached plugin for MySQL.
* It allows us to do interesting things like access the key-value store as
it was at some time in the past.

In order to view the key-value store as it was sometime in the past, simply
pass in a datetime value as `snapshot_time` when you instantiate a dict.
You now have a read-only interface to all of the keys and values in the store
as they were at that time.

If you don't need this functionality and don't want your table to continue
to grow in size as you write, you can pass the parameter
`prune_on_write = True` when you instantiate the dict. This will result in
all old copies of a key being deleted when a key-value pair is written.

At this time, two SqlDicts are provided-- `GenericSqlDict` and `MySqlDict`. 
Currently they differ only in the declared types of table columns. In the 
future backends targeted towards other RDBMS's may be added in order to take
define column types with greater granularity. In addition, RDBMS specific 
backends may undergo tuning of queries to provide best performance on each 
platform.

It's a good idea to pay attention to the `engine_kwargs` argument when 
instantiating a SqlDict. If used in a highly threaded environment it may
be useful to increase the `pool_size` and `max_overflow` values. When used
with HAProxy or a firewall that drops idle connections, setting a short
`pool_recycle` value can prevent headaches. See the 
[sqlalchemy documentation](http://docs.sqlalchemy.org/en/latest/core/engines.html#engine-creation-api) 
for a list of parameters and what they mean. 

**Important:** The code relies on the autoincrementing integer column 
`sequence_number` to be **sequential**. This is typically not a problem, but
certain database configurations, for example galera replication with 
MariaDB/Percona, do not guarantee autoincrement fields to be strictly 
sequential when writes occur across multiple hosts. In this case, you can turn 
on `prune_on_write`, or you can choose to only write to one host. If you'd like
to only write to one host but still take advantage of read replicas, try using
the `TieredStorageDict` with your read replica(s) wrapped in a `ReadOnlyDict` 
(and perhaps `RandomChoiceDict` if you want to balance your reads across multiple
replicas without using HAProxy or proxysql) in front of SqlDicts pointing to a 
single node.  


### Utility

#### Read Only and Write Only

The `ReadOnlyDict` and `WriteOnlyDict` accept an instance of a dict and wrap it
to prevent writing or reading. The `ReadOnlyDict` silently discards writes and 
deletes. The `WriteOnlyDict` raises KeyError on any attempts to access keys, 
reports a length of zero, and yields and empty set if you attempt to iterate it.
 

#### Serialized 

All mutable mappings in this module expect all keys and values to be strings. They 
might incidentally work with other types, but really you should be using strings. 
If you want to store other types and you don't want to have to serialize and 
deserialize each time you write or read from the dict, you can wrap the instance in 
a `SerializedDict`.

At instantiation, the `SerializedDict` takes a dict to wrap and key and value 
serialization and deserialization functions. Like this--

```
my_dict = GenericSqlDict('sqlite://:memory:', 'some_table')
my_serialized_dict = SerializedDict(my_dict, 
                                    key_serialize = pickle.dumps, 
                                    key_deserialize = pickle.loads,
                                    value_serialize = zlib.compress, 
                                    value_deserialize = zlib.decompress)
```

#### Random Choice 

Provides `RandomChoiceDict` which wraps a list of dict instances. Any reads or writes
are performed on a random instance from the list.


#### Tiered Storage

Offers `TieredStorageDict` for stacking dictionary-like key-value
storage objects.

At instantiation, it accepts a list of key-value
store instances.

Attempts to read start at the head of the storage
backends list and iterate towards the tail of the list until either the
item is found or the end of the list is reached, at which time a
KeyError will be thrown. Writes to tiered storage will begin writing at
the tail of the storage backend list and iterate towards the front of
the list.


## Example

Let's say you're running a high volume site for storing text files. You have 
three tiers of storage-- 
* Redis with a single master node and read replicas on each application node for 
microcaching of hot documents
* A MySQL cluster with a single master node and several read replicas for storage
of "active" documents and their revision history
* An NFS mount for the storage of archived documents

Your key-value storage dictionary on an application node might look something like
this--

```
import redis
import zlib
from shitty_tools.key_value.filesystem import FileSystemDict
from shitty_tools.key_value.redis import RedisDict
from shitty_tools.key_value.sql.mysql import MySqlDict
shitty_tools.key_value.utility import (ReadOnlyDict, WriteOnlyDict,
                                       SerializedDict, RandomChoiceDict, 
                                       TieredStorageDict)


redis_replica_conn = redis.Redis('localhost')
redis_master_conn = redis.Redis('redis-master')
# zlib compress documents stored in redis to save memory
redis_read_dict = ReadOnlyDict(SerializedDict(RedisDict(redis_replica_conn),
                                              value_deserializer = zlib.decompress))
redis_write_dict = WriteOnlyDict(SerializedDict(RedisDict(redis_master_conn, exp_time = 5),
                                                value_serializer = zlib.compress))
redis_dict = TieredStorage([redis_read_dict, redis_write_dict])
# We want to load balance reads between eight replicas with hostnames mysql_0 through mysql_7 
db_read_dict = RandomChoiceDict([ReadOnlyDict(MySqlDict('mysql://mysql_%s/text_db?charset=utf8' %n, 'text_docs'))
                                 for n in range(8)])
db_write_dict = WriteOnlyDict(MySqlDict('mysql://mysql_master/text_db?charset=utf8', 'text_docs'))
db_dict = TieredStorage([db_read_dict, db_write_dict])
# Even though we're not doing writes, we still need to specify a scratch directory
fs_dict = ReadOnlyDict(FileSystemDict('/mnt/archive/documents', '/mnt/archive/scratch'))
# Final dict with each tier of storage
document_dict = TieredStorage([redis_dict, db_dict, fs_dict])
```

