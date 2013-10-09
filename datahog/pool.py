# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import bisect
import contextlib
import Queue
import random
import time

try:
    import greenhouse
except ImportError:
    greenhouse = None
else:
    import greenhouse.ext.psycopg2 as greenpsycopg2

try:
    import gevent
except ImportError:
    gevent = None
else:
    import gevent.core
    import gevent.event
    import gevent.queue
    import gevent.socket

import psycopg2
import psycopg2.extensions

from . import error
from .const import util

__all__ = []


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

        ``lookup_insertion_plans``
            Lists of lists of two-tuples of shard numbers, and their integer
            weights. This is used for the associated lookup tables of aliases
            and names.

            Each of these sub-lists is an "insertion plan", and the last plan
            is always used to calculate the shard on which a new lookup table
            row will be stored.

            For lookups inserted by older insertion plans to work, you can
            never remove or change any insertion plans that made it into
            production. To change weights or the list of shards being inserted
            into, append a new plan to the list.

        ``entity_insertion_plan``
            A list of two-tuples of shard number and weight, used for a
            weighted random choice of shard for inserting a new entity. This
            key is optional, the full list of shards will be used by default.

        ``shard_bits``
            Number of bits at the top of auto-incrementing 64-bit ints to
            reserve for shard number. 8 is a good value -- allows for up to 256
            shards, and auto-increments (per shard) up to ``2**56``.

        ``digest_key``
            Lookups by strings will be sharded on the HMAC digest of the
            string. Provide the key here.

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

        self._init_conf()

        self.shardbits = self._dbconf['shard_bits']
        self.digestkey = self._dbconf['digest_key']

    def _init_conf(self):
        conf = self._dbconf

        for key in ('shards', 'shard_bits', 'lookup_insertion_plans',
                'digest_key'):
            if not conf.get(key, None):
                raise Exception("missing or empty required key %r" % key)

        for plan in conf['lookup_insertion_plans']:
            _prepare_plan(plan)

        for shard in conf['shards']:
            for key in ('shard', 'count', 'host', 'port', 'user', 'password',
                    'database'):
                if key not in shard:
                    raise Exception("missing shard dict key %r" % key)

        if 'entity_insertion_plan' not in conf:
            conf['entity_insertion_plan'] = [(s['shard'], 1)
                    for s in conf['shards']]
        _prepare_plan(conf['entity_insertion_plan'])

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
        return guid >> (64 - self.shardbits)

    def shards_for_lookup_hash(self, digest):
        num = _int_hash(digest)
        seen = set()
        for plan in self._dbconf['lookup_insertion_plans'][::-1]:
            shard = _pick_from_plan(digest, plan, num)
            if shard in seen:
                continue
            seen.add(shard)
            yield shard

    def shards_for_lookup_prefix(self, value):
        num = ord(value[0])
        seen = set()
        for plan in self._dbconf['lookup_insertion_plans'][::-1]:
            shard = _pick_from_plan(None, plan, num)
            if shard in seen:
                continue
            seen.add(shard)
            yield shard

    def shard_for_alias_write(self, digest):
        return _pick_from_plan(digest,
                self._dbconf['lookup_insertion_plans'][-1])

    def shard_for_prefix_write(self, value):
        return _pick_from_plan(None,
                self._dbconf['lookup_insertion_plans'][-1], ord(value[0]))

    # pass in the dmetaphone code, then these implementations are identical
    shard_for_phonetic_write = shard_for_prefix_write

    def shard_for_entity_write(self):
        plan = self._dbconf['entity_insertion_plan']
        rand = random.randrange(plan[-1][0])
        index = bisect.bisect_right(plan, (rand, 99999999999))
        return plan[index][1]

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

    def get_for_entity_write(self, replace=True, timeout=None):
        return self.get_by_shard(
                self.shard_for_entity_write(), replace, timeout)

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


if gevent:
    __all__.append("GeventConnPool")

    def _gevent_wait_callback(conn):
        while 1:
            state = conn.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_READ:
                gevent.socket.wait_read(conn.fileno())
            elif state == psycopg2.extensions.POLL_WRITE:
                gevent.socket.wait_write(conn.fileno())
            else:
                raise psycopg2.OperationalError(
                        'Bad result from poll: %r' % state)

    # gevent doesn't have a monkey-patching equivalent of threading.Timer?!
    class _gevent_timer(object):
        def __init__(self, timeout, func):
            self._timeout = timeout
            self._func = func
            self._timer = None

        def start(self):
            if self._timer is None:
                self._timer = gevent.core.timer(self._timeout, self._func)

        def cancel(self):
            if self._timer is not None:
                self._timer.cancel()

    class GeventConnPool(ConnectionPool):
        '''a :class:`ConnectionPool` that uses gevent_ for blocking calls

        .. _gevent: http://www.gevent.org
        '''
        @staticmethod
        def _background(f):
            gevent.spawn(f)

        @staticmethod
        def _q():
            return gevent.queue.Queue()

        @staticmethod
        def _ev():
            return gevent.event.Event()

        def start(self):
            psycopg2.extensions.set_wait_callback(_gevent_wait_callback)
            super(GeventConnPool, self).start()

        _timer = _gevent_timer


def _int_hash(digest):
    n = 0
    for c in digest:
        n <<= 8
        n |= ord(c)
    return n

def _pick_from_plan(digest, plan, num=None):
    if num is None:
        num = _int_hash(digest)
    index = bisect.bisect_right(plan, (num % plan[-1][0], 999999999))
    return plan[index][1]

# convert a [(shard, weight)] plan to a [(partialsum, shard)] plan
def _prepare_plan(plan):
    partial = 0
    for i, (shard, weight) in enumerate(plan):
        partial += weight
        plan[i] = (partial, shard)
