from sqlalchemy import Column, Index, MetaData, Table, text
from sqlalchemy.dialects.mysql.base import LONGBLOB, BIGINT, TINYINT, VARCHAR, TIMESTAMP
from generic_sql import GenericSqlDict


class MySqlDict(GenericSqlDict):
    def _generate_kv_table_object(self, table_name, key_length):
        metadata = MetaData()
        t_kv = Table(
            table_name, metadata,
            Column('key', VARCHAR(key_length), nullable=False),
            Column('sequence_number', BIGINT(30), primary_key=True),
            Column('value', LONGBLOB),
            Column('is_deleted', TINYINT(1), nullable=False, server_default=text("'0'")),
            Column('created', TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
            Index('key_seq_uq', 'key', 'sequence_number', unique=True)
        )
        return t_kv