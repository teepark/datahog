# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import hashlib
import re
import sys

import psycopg2


__all__ = ["activate", "deactivate", "reset", "connect_fail", "query_fail",
        "add_fetch_result", "eventlog", "CONNECT", "CONNECT_FAIL",
        "GET_CURSOR", "COMMIT", "ROLLBACK", "RESET", "TPC_BEGIN", "TPC_COMMIT",
        "TPC_ROLLBACK", "TPC_PREPARE", "FETCH_ONE", "FETCH_ALL", "ROWCOUNT",
        "EXECUTE", "EXECUTE_FAILURE"]


def activate():
    psycopg2.connect = fake_connect

def deactivate():
    psycopg2.connect = real_connect

def reset():
    del eventlog[:]
    _fetch[0] = -1
    del _fetch[1][:]
    connect_fail(None)
    query_fail(None)

_connect_fail = False
_query_fail = None
_fetch = [-1, []]

def connect_fail(flag=False):
    global _connect_fail
    _connect_fail = flag

def query_fail(exc_klass):
    global _query_fail
    _query_fail = exc_klass

def add_fetch_result(result):
    _fetch[1].append(result)


# log a record of every pg-related action taken, for matching later
eventlog = []
_log = eventlog.append

class pgevent(object):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<%s>' % (self.name,)

# event types
CONNECT = pgevent("CONNECT")
CONNECT_FAIL = pgevent("CONNECT_FAIL")
GET_CURSOR = pgevent("GET_CURSOR")
COMMIT = pgevent("COMMIT")
ROLLBACK = pgevent("ROLLBACK")
RESET = pgevent("RESET")
TPC_BEGIN = pgevent("TPC_BEGIN")
TPC_COMMIT = pgevent("TPC_COMMIT")
TPC_ROLLBACK = pgevent("TPC_ROLLBACK")
TPC_PREPARE = pgevent("TPC_PREPARE")
FETCH_ONE = pgevent("FETCH_ONE")
FETCH_ALL = pgevent("FETCH_ALL")
ROWCOUNT = pgevent("ROWCOUNT")
class EXECUTE(object):
    def __init__(self, pattern, args):
        self.pattern = _spaces.sub('', pattern)
        self.args = tuple(args)

    def __repr__(self):
        return '<%s pattern: %s, args: %r>' % (
                type(self).__name__,
                hashlib.sha1(self.pattern).hexdigest()[:5],
                self.args)

    def __eq__(self, other):
        return (isinstance(other, type(self)) and
                self.pattern == other.pattern and
                self.args == other.args)
class EXECUTE_FAILURE(EXECUTE):
    pass


class FakePGConn(object):
    def cursor(self):
        _log(GET_CURSOR)
        return FakePGCursor()

    def commit(self): _log(COMMIT)
    def rollback(self): _log(ROLLBACK)
    def reset(self): _log(RESET)
    def tpc_begin(self, xid): _log(TPC_BEGIN)
    def tpc_commit(self, xid): _log(TPC_COMMIT)
    def tpc_rollback(self, xid=None): _log(TPC_ROLLBACK)
    def tpc_prepare(self): _log(TPC_PREPARE)

    def __enter__(self):
        return self

    def __exit__(self, klass=None, exc=None, tb=None):
        if exc is not None:
            _log(ROLLBACK)
        else:
            _log(COMMIT)

    def xid(self, *args):
        return ''.join(map(repr, args))


class FakePGCursor(object):
    def execute(self, pattern, args=()):
        args = tuple(
                x.adapted if isinstance(x, type(psycopg2.Binary(''))) else x
                for x in args)
        if _query_fail is not None:
            _log(EXECUTE_FAILURE(pattern, args))
            raise _query_fail()
        _log(EXECUTE(pattern, args))
        _fetch[0] += 1

    def fetchone(self):
        _log(FETCH_ONE)
        if not _fetch:
            return None
        i = _fetch[0]
        return _fetch[1][i].pop(0)

    def fetchall(self):
        _log(FETCH_ALL)
        i = _fetch[0]
        results = _fetch[1][i][:]
        _fetch[1][i][:] = []
        return results

    @property
    def rowcount(self):
        _log(ROWCOUNT)
        i = _fetch[0]
        return len(_fetch[1][i])


real_connect = psycopg2.connect

def fake_connect(host, port, user, password, database):
    if _connect_fail:
        _log(CONNECT_FAIL)
        raise psycopg2.OperationalError()

    _log(CONNECT)
    return FakePGConn()

_spaces = re.compile('\s')
