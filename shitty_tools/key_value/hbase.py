from collections import MutableMapping


class HbaseDict(MutableMapping):
    def __init__(self, connection_pool, table_name, value_column):
        '''
        Takes a happybase connection pool, a table name, and the name of a value column
        and provides a key-value store interface to the HBase column.
        
        If the table doesn't exist, you'll need to create it first.
        
        Only columns that contain values are returned. It is possible to have two dicts
        referencing different columns in the same table.
        

        >>> import happybase
        >>> pool = happybase.ConnectionPool(size=1, host='natkinson-dev.abc.local')
        >>> c = pool._acquire_connection()
        >>> c.create_table('bar', {'v0:value1': {}, 'v1:value2': {}})
        >>> pool._return_connection(c)
        >>> v0 = hbase.HbaseDict(pool, 'bar', 'v0:value0')
        >>> v0['foo'] = 'bar'
        >>> v0.keys()
        ['foo']
        >>> v1 = hbase.HbaseDict(pool, 'bar', 'v1:value1')
        >>> v1.keys()
        []
        >>> v1['foo'] = 'bar1'
        >>> v1.keys()
        ['foo']
        >>> v0.keys()
        ['foo']
        >>> v0['foo']
        'bar'
        >>> v1['foo']
        'bar1'
        >>> del(v1['foo'])
        >>> v0['foo']
        'bar'
        
        
        This does not *currently* support handling broken connections. It is planned.
        Currently behaviour follows underlying happybase behaviour--
        https://happybase.readthedocs.io/en/latest/user.html#handling-broken-connections      
        '''
        self.pool = connection_pool
        self.value_column = value_column
        self.table_name = table_name

    def __getitem__(self, key):
        with self.pool.connection() as conn:
            try:
                return conn.table(self.table_name).row(key, (self.value_column,))[self.value_column]
            except KeyError:
                # Happybase returns an empty dict if the row isn't found. Trying to do the column
                # look-up on the empty dict will cause a KeyError
                raise KeyError(key)

    def __iter__(self):
        with self.pool.connection() as conn:
            for rowkey, rowdata in conn.table(self.table_name).scan(columns=(self.value_column,)):
                try:
                    _ = rowdata[self.value_column]
                    yield rowkey
                except KeyError:
                    # This shouldn't happen, but it could if the user gave us a valid table and an
                    # invalid value column
                    pass

    def __len__(self):
        # Meh. Looks like there's no obvious way to get this without a full table scan. Have fun.
        n = -1
        for n, _ in enumerate(self.__iter__()): pass
        return n + 1

    def __setitem__(self, key, value):
        with self.pool.connection() as conn:
            conn.table(self.table_name).put(key, {self.value_column: value})

    def __delitem__(self, key):
        with self.pool.connection() as conn:
            conn.table(self.table_name).delete(key, (self.value_column,))
