# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import contextlib
import Queue
import random
import threading
import time

try:
    import greenhouse
except ImportError:
    greenhouse = None
else:
    import greenhouse.ext.psycopg2 as greenpsycopg2
import psycopg2

from . import error

__all__ = ["ConnectionPool"]


class ConnectionPool(object):
    '''An object wrapping a pool-per-shard for a sharded group of DBs

    :param dict dbconf:
        Postgresql connection information/configuration for the whole cluster.

        Must contain certain keys:

        ``shards``
            A list of dictionaries with connection credentials. Required in
            these dicts:

            - ``shard``: shard number
            - ``count``: number of connections to build for this shard
            - ``host``: hostname of the db
            - ``port``: db's port
            - ``user``: username to connect with
            - ``password``: user's password
            - ``database``: database name

        ``shard_bits``
            Number of bits at the top of auto-incrementing 64-bit ints to
            reserve for shard number. 8 is a good value -- allows for up to 256
            shards, and auto-increments (per shard) up to ``2**56``.

        ``alias_insertion_plans``
            Lists of lists of shard numbers. Initially, the outer list should
            only contain one sub-list, and this is the list of shard numbers on
            which aliases should be stored.

            When you add shards to your cluster and want to start storing
            aliases in the new ones (or want to take some shards out of the
            write pool for new aliases), you must keep the old
            alias_insertion_plans sub-lists in tact, and add a new plan to the
            end.

    :param bool readonly:
        Whether to disallow data-modifying methods against this connection
        pool. Can be useful for querying replication slaves to take some read
        load off of the masters (default ``False``).
    '''

    def __init__(self, dbconf, readonly=False):
        self.readonly = readonly
        self._dbconf = dbconf
        self._conns = {}
        self._out = {}
        self._ready_evs = []

    def start(self):
        '''Initiate the DB connections

        This method won't block, use :meth:`wait_ready` to wait until the
        connections have all been established.
        '''
        for shard in self._dbconf['shards']:
            self._conns[shard['shard']] = self._q()
            for i in xrange(shard['count']):
                ev = self._ev()
                self._ready_evs.append(ev)
                self._start_conn(shard, ev)

    def wait_ready(self, timeout=None):
        '''Block until all dB connections are ready
        
        :param timeout:
            Maximum time to wait in seconds (the default None means no limit).

        :returns:
            A boolean indicating that all connections were successfully made.
        '''
        if timeout is not None:
            now = time.time()
            deadline = now + timeout

        for ev in self._ready_evs:
            if timeout is None:
                ev.wait()
            else:
                ev.wait(deadline - now)
                now = time.time()
                if now > deadline:
                    return False

        return True

    def put(self, conn):
        shard = self._out.pop(id(conn))
        self._conns[shard].put(conn)

    def shard_by_guid(self, guid):
        return guid >> (64 - self._dbconf['shard_bits'])

    def shards_for_alias_hash(self, digest):
        byte = ord(digest[-1])
        seen = set()
        for plan in self._dbconf['alias_insertion_plans'][::-1]:
            shard = plan[byte % len(plan)]
            if shard in seen:
                continue
            seen.add(shard)
            yield shard

    def shard_for_alias_write(self, digest):
        byte = ord(digest[-1])
        plan = self._dbconf['alias_insertion_plans'][-1]
        return plan[byte % len(plan)]

    def random_shard(self):
        return random.choice(self._dbconf['shards'])['shard']

    def get_by_shard(self, shard, replace=True, timeout=None):
        if shard not in self._conns:
            raise error.NoShard(shard)

        if timeout is not None:
            deadline = time.time() + timeout

        try:
            conn = self._conns[shard].get(timeout)
        except Queue.Empty:
            raise error.Timeout()

        if timeout is not None:
            timeout = deadline - time.time()

        self._out[id(conn)] = shard

        if replace:
            if timeout is not None:
                conn = self._timeout_context(conn, timeout)
            conn = self._replacement_context(conn)

        return conn

    def get_by_guid(self, guid, replace=True, timeout=None):
        return self.get_by_shard(self.shard_by_guid(guid), replace, timeout)

    def get_random(self, replace=True, timeout=None):
        return self.get_by_shard(self.random_shard(), replace, timeout)

    _timer = threading.Timer

    @contextlib.contextmanager
    def _replacement_context(self, conn):
        try:
            with conn as c:
                yield c
        finally:
            self.put(c)

    @contextlib.contextmanager
    def _timeout_context(self, conn, timeout):
        t = self._timer(timeout, conn.cancel)
        t.start()
        try:
            with conn:
                yield conn
        except psycopg2.extensions.QueryCanceledError:
            conn.reset()
            raise error.Timeout()
        else:
            t.cancel()

    def _start_conn(self, shard, done):
        @self._background
        def f():
            conn = psycopg2.connect(
                    host=shard['host'],
                    port=shard['port'],
                    user=shard['user'],
                    password=shard['password'],
                    database=shard['database'])
            self._conns[shard['shard']].put(conn)
            done.set()

    @staticmethod
    def _background(f):
        threading.Thread(target=f).start()

    @staticmethod
    def _q():
        return Queue.Queue()

    @staticmethod
    def _ev():
        return threading.Event()


if greenhouse:
    __all__.append("GreenhouseConnPool")

    class GreenhouseConnPool(ConnectionPool):
        '''a :class:`ConnectionPool` that uses greenhouse_ for blocking calls

        .. _greenhouse: http://teepark.github.io/greenhouse
        '''
        @staticmethod
        def _background(f):
            greenhouse.schedule(f)

        @staticmethod
        def _q():
            return greenhouse.Queue()

        @staticmethod
        def _ev():
            return greenhouse.Event()

        def start(self):
            psycopg2.extensions.set_wait_callback(greenpsycopg2.wait_callback)
            super(GreenhouseConnPool, self).start()

        _timer = greenhouse.Timer
