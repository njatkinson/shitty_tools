import datetime
from collections import MutableMapping
from random import choice
from sqlalchemy import create_engine, insert, select, desc
from sqlalchemy import BigInteger, Column, DateTime, Index, Integer, MetaData, String, Table, text, LargeBinary
from sqlalchemy.orm import sessionmaker, scoped_session


class GenericSqlDict(object):
    def __init__(self, connection_string, table_name, key_length = 255, engine_kwargs= {},
                 serializer = lambda x: x, deserializer = lambda x: x,
                 snapshot_time = None, prune_on_write = False,
                 read_replica_connection_string_list = [], read_replica_engine_kwargs = {}):
        write_engine = create_engine(connection_string, **engine_kwargs)
        self._kv_table = self._generate_kv_table_object(table_name, key_length)
        if not write_engine.has_table(table_name):
            self._kv_table.create(bind = write_engine)

        self._get_write_session = self._construct_session_factory(write_engine)
        if not read_replica_connection_string_list:
            self._get_read_session = self._construct_session_factory(write_engine)
        else:
            read_session_factories = [self._construct_session_factory(create_engine(read_replica_connection_string,
                                                                                    **read_replica_engine_kwargs))
                                      for read_replica_connection_string in read_replica_connection_string_list]
            self._get_read_session = lambda: choice(read_session_factories)()

        self._serializer = serializer
        self._deserializer = deserializer

        # TODO: snapshots, read only mode, prune on write


    def _construct_session_factory(self, engine):
        session_maker = sessionmaker(engine)
        return lambda: scoped_session(session_maker)


    def _generate_kv_table_object(self, table_name, key_length):
        metadata = MetaData()
        t_kv = Table(
            table_name, metadata,
            Column('key', String(key_length), nullable=False),
            Column('sequence_number', BigInteger, primary_key=True),
            Column('value', LargeBinary),
            Column('is_deleted', Integer, nullable=False, default=False),
            Column('created', DateTime, default=datetime.datetime.utcnow),
            Index('key_seq_uq', 'key', 'sequence_number', unique=True)
        )
        return t_kv


    def _generate_insert_statement(self, key, value):
        return insert(self._kv_table, values = {'key': key, 'value': self._serializer(value)})


    def _write(self, key, value):
        insert_statement = self._generate_insert_statement(key, value)
        with self._get_write_session().no_autoflush as session:
            session.execute(insert_statement)
            session.commit()


    def _generate_select_key_statement(self, key):
        return select([self._kv_table.c.value, self._kv_table.c.is_deleted]).where(self._kv_table.c.key == key).\
            order_by(desc(self._kv_table.c.sequence_number)).limit(1)


    def _read(self, key):
        select_statement = self._generate_select_key_statement(key)
        with self._get_read_session().no_autoflush as session:
            result = session.execute(select_statement).fetchone()
            if result is None:
                raise KeyError
            if result.is_deleted:
                raise KeyError
            return result.value