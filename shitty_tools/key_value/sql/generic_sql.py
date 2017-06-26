import datetime
from collections import MutableMapping
from sqlalchemy import create_engine, insert, select, desc, func, and_, alias
from sqlalchemy import BigInteger, Column, DateTime, Index, Integer, MetaData, String, Table, text, LargeBinary
from sqlalchemy.orm import sessionmaker, scoped_session


class GenericSqlDict(MutableMapping):
    def __init__(self, connection_string, table_name, key_length = 255, engine_kwargs= {},
                 serializer = lambda x: x, deserializer = lambda x: x,
                 snapshot_time = None, prune_on_write = False):
        write_engine = create_engine(connection_string, **engine_kwargs)
        self._kv_table = self._generate_kv_table_object(table_name, key_length)
        if not write_engine.has_table(table_name):
            self._kv_table.create(bind = write_engine)

        self._get_session = self._construct_session_factory(write_engine)

        self._serializer = serializer
        self._deserializer = deserializer

        # TODO: snapshots, prune on write


    def _construct_session_factory(self, engine):
        session_maker = sessionmaker(engine)
        return lambda: scoped_session(session_maker)


    def _generate_kv_table_object(self, table_name, key_length):
        # TODO: Rather than a global sequence number, do sequence number per key
        metadata = MetaData()
        t_kv = Table(
            table_name, metadata,
            Column('label', String(key_length), nullable=False),
            Column('sequence_number', BigInteger, primary_key=True),
            Column('item', LargeBinary),
            Column('is_deleted', Integer, nullable=False, default=False),
            Column('created', DateTime, default=datetime.datetime.utcnow),
            Index('label_seq_uq', 'label', 'sequence_number', unique=True),
            Index('is_deleted_idx', 'is_deleted')
        )
        return t_kv


    def _generate_insert_statement(self, key, value, is_deleted):
        return insert(self._kv_table, values = {'label': key,
                                                'item': self._serializer(value),
                                                'is_deleted': is_deleted})


    def _write(self, key, value, is_deleted):
        insert_statement = self._generate_insert_statement(key, value, is_deleted)
        with self._get_session().no_autoflush as session:
            session.execute(insert_statement)
            session.commit()
        session.close()


    def _generate_select_key_statement(self, key):
        return select([self._kv_table.c.item, self._kv_table.c.is_deleted]).where(self._kv_table.c.label == key).\
            order_by(desc(self._kv_table.c.sequence_number)).limit(1)


    def _generate_select_all_keys_statement(self, count_only = False):
        # TODO: This query could probably be more efficient
        lhs = alias(self._kv_table, 'lhs')
        rhs = alias(self._kv_table, 'rhs')
        if count_only:
            fields = [func.count(lhs.c.label)]
        else:
            fields = [lhs.c.label]
        select_statement = select(fields).\
            select_from(lhs.
                        outerjoin(rhs,
                                  and_(rhs.c.label == lhs.c.label,
                                       rhs.c.sequence_number > lhs.c.sequence_number))).\
            where(and_(rhs.c.label.is_(None),
                       lhs.c.is_deleted == 0))
        return select_statement


    def _read(self, key):
        select_statement = self._generate_select_key_statement(key)
        with self._get_session().no_autoflush as session:
            result = session.execute(select_statement).fetchone()
            if result is None:
                raise KeyError
            if result.is_deleted:
                raise KeyError
        session.close()
        return result.item


    def __getitem__(self, key):
        return self._read(key)


    def __setitem__(self, key, value):
        return self._write(key, value, False)


    def __delitem__(self, key):
        return self._write(key, None, True)


    def __iter__(self):
        select_statement = self._generate_select_all_keys_statement()
        with self._get_session().no_autoflush as session:
            for row in session.execute(select_statement):
                yield row.label
        session.close()


    def __len__(self):
        select_statement = self._generate_select_all_keys_statement(count_only=True)
        with self._get_session().no_autoflush as session:
            result = session.execute(select_statement).scalar()
        session.close()
        return result