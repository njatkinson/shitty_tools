from sqlalchemy import Column, Index, MetaData, Table, text
from sqlalchemy.dialects.mysql.base import LONGBLOB, BIGINT, TINYINT, VARCHAR, TIMESTAMP
from generic_sql import GenericSqlDict


class MySqlDict(GenericSqlDict):
    def _generate_kv_table_object(self, table_name, key_length):
        metadata = MetaData()
        t_kv = Table(
            table_name, metadata,
            Column('label', VARCHAR(key_length), nullable=False),
            Column('sequence_number', BIGINT(30), primary_key=True),
            Column('item', LONGBLOB),
            Column('is_deleted', TINYINT(1), nullable=False, server_default=text("'0'")),
            Column('created', TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
            Index('key_seq_uq', 'label', 'sequence_number', unique=True),
            Index('is_deleted_idx', 'is_deleted')
        )
        return t_kv