#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = 'SpringChan'

'''
Database operation module
'''

import time, uuid, functools, threading, logging

class Dict(dict):
    '''
    Simple dict but support access as x.y style
    '''

    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key];
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


def next_id(t=None):
    '''
    Return next id as 50-char string.

    Args:
        t: unix timestamp, default to None and using time.time()
    '''

    if t is None:
        t = time.time()

    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)

def _profiling(start, sql=''):
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))

class DBError(Exception):
    pass

class MultiColumnsError(DBError):
    pass

class _LazyConnection(object):
    def __init__(self):
       self.connection = None

    def cursor(self):
       if self.connection is None:
           self.connection = engine.connect()
           logging.info('open connection <%s>' % hex(id(self.connection)))
       return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            self.connection.close()
            logging.info('close connection')


class _DBCtx(threading.local):
    '''
    Thread local object that holds connection info.
    '''

    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection ...')
        self.connection = _LazyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()


_db_ctx = _DBCtx()

engine = None

class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()


def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')

    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults= dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)

    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)

    params['buffered'] = True

    engine = _Engine(lambda: mysql.connector.connect(**params))

    #test connection...
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))


class _ConnectionCtx(object):
    '''
    _ConnectionCtx object that open and close connection context. _ConnectionCtx object can be nested and only the most outer connection has effect.

    with connection():
        pass
        with connection():
            pass
    ...
    '''
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, trackback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def connection():
    return _ConnectionCtx()

def with_connection(func):
    '''
    Decorator _ConnectionCtx object that can be used by 'with' statement:

    @with_connection()
    def foo(*arg, **kw):
        f1()
        f2()
        f3()
    ...
    '''

    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper


class _TransactionCtx(object):
    '''
    _TransactionCtx object that can handle transactions.

    with _TransactionCtx():
        pass
    ...

    '''

    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True

        _db_ctx.transactions = _db_ctx.transactions + 1

        return self

    def __exit__(self, exctype, excvalue, trackback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1

        try:
            if _db_ctx.transactions == 0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_cleanup:
                _db_ctx.cleanup()


    def commit(self):
        global _db_ctx
        logging.info('commit transaction ...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok')
        except:
            logging.warning('commit failed. try rollback')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning("rollback transaction ...")
        _db_ctx.connection.rollback()
        logging.warning('rollback ok')


def transaction():
    return _TransactionCtx()

def with_transaction(func):
    '''
    Decorator _TransactionCtx object that can be used by 'with' statement:

    @with_transaction()

    '''
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kw)
        _profiling(_start)
    return _wrapper


def _select(sql, first, *args):
    'execute select SQL and return unique or list results.'

    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

@with_connection
def select_one(sql, *args):
    return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
    result = _select(sql, True, *args)
    if len(result) != 1:
        raise MultiColumnsError('Expect only one column')
    return result.values()[0]

@with_connection
def select(sql, *args):
    return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL :%s, ARGS:%s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount

        if _db_ctx.transactions == 0:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def insert(table, **kw):
    print kw
    cols, args = zip(*kw.iteritems())
    sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    print sql
    return _update(sql, *args)

def update(sql, *args):
    return _update(sql, *args)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine('root', '123456', 'test')
    update('DROP TABLE IF EXISTS user')
    update('create table user (id int  primary key, name text, email text, passwd text, last_modified real)')
    update("insert into `user` (id, name, email, passwd) values(?,?,?,?)", 1, 'chanchun', 'cc0107@163.com', '123456')
    insert('user', id=2, name='SpringChan', email='linuxwingis@gmail.com', passwd='chenchun ')
