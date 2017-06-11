import Queue
from contextlib import contextmanager


# TODO: Implement connection max age & recycling


class ConnectionPool(object):
    def __init__(self, constructor, pool_size, overflow = 0, timeout = 60, destructor = lambda x: x):
        '''
        Generic connection pool class.

        Give it a constructor for a session and tell it how big you want the pool to be.
        Something like this--

        my_pool = ConnectionPool(lambda: get_some_shitty_conn(some_shitty_arg), pool_size = 6)

        If the connections don't get closed cleanly by garbage collection and need to be
        explicitly closed, you can pass in an optional destructor. Something like this--

        my_pool = ConnectionPool(lambda: get_some_shitty_conn(some_shitty_arg), pool_size = 6,
                                 destructor = lambda conn: conn.close())

        At instantiation, the pool is empty. Connections are lazily created until pool_size is
        reached. Once pool size is reached, no new connections are created by default.

        If you set an overflow value, up to `overflow` extra connections can be created. In an
        overflow situation, sessions are returned to the pool until the pool is full, at which
        time, extra connections are discarded rather than returned to the pool.

        Use the `session_context` context manager to access your sessions. Like this--

        with my_pool.session_context() as my_session:
            my_session.do_some_shit()

        Alternatively, use the explicit get_session/release_session methods if you're a dumbass.

        :param constructor: function that creates a connection
        :param pool_size: how many connections you want to keep open
        :param overflow: number of one-off connections to allow when pool is full
        :param timeout: max time to wait for a connection
        :param destructor: function that accepts a connection as argument and cleanly closes it
        '''
        self.session_constructor = constructor
        self.conn_queue = Queue.Queue(pool_size)
        max_conns = pool_size + overflow
        self.conn_tokens = Queue.Queue(max_conns)
        for _ in xrange(max_conns):
            self.conn_tokens.put(None)
        self.conn_timeout = timeout
        self.destructor = destructor


    def get_session(self):
        self.conn_tokens.get(timeout=self.conn_timeout)
        try:
            conn = self.conn_queue.get_nowait()
        except Queue.Empty:
            conn = self.session_constructor()
        return conn


    def release_session(self, conn):
        try:
            self.conn_queue.put_nowait(conn)
        except Queue.Full:
            self.destructor(conn)
        try:
            self.conn_tokens.put_nowait(None)
        except Queue.Full:
            pass


    @contextmanager
    def session_context(self):
        conn = self.get_session()
        try:
            yield conn
        finally:
            self.release_session(conn)