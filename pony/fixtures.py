import os
import logging

from pony.py23compat import PY2
from ponytest import with_cli_args, pony_fixtures

from functools import wraps
import click
from contextlib import contextmanager, closing

from pony.utils import cached_property, class_cached_property

from pony.orm.dbproviders.mysql import mysql_module
from pony.utils import cached_property, class_property

if not PY2:
    from contextlib import contextmanager
else:
    from contextlib2 import contextmanager

from pony.orm import db_session, Database, rollback


class DBContext(object):

    class_scoped = True

    def __init__(self, test_cls):
        test_cls.db_fixture = self
        test_cls.db = class_property(lambda cls: self.db)
        test_cls.db_provider = class_property(lambda cls: self.provider)
        self.test_cls = test_cls

    @class_property
    def fixture_name(cls):
        return cls.provider

    def init_db(self):
        raise NotImplementedError

    @cached_property
    def db(self):
        raise NotImplementedError

    def __enter__(self):
        self.init_db()
        self.test_cls.make_entities()
        self.db.generate_mapping(check_tables=True, create_tables=True)

    def __exit__(self, *exc_info):
        self.db.provider.disconnect()


    @classmethod
    @with_cli_args
    @click.option('--db', '-d', 'database', multiple=True)
    @click.option('--exclude-db', '-e', multiple=True)
    def invoke(cls, database, exclude_db):
        fixture = [
            MySqlContext, OracleContext, SqliteContext, PostgresContext,
            SqlServerContext,
        ]
        all_db = [ctx.provider for ctx in fixture]
        for db in database:
            if db == 'all':
                continue
            assert db in all_db, (
                "Unknown provider: %s. Use one of %s." % (db, ', '.join(all_db))
            )
        if 'all' in database:
            database = all_db
        elif exclude_db:
            database = set(all_db) - set(exclude_db)
        elif not database:
            database = ['sqlite']
        for Ctx in fixture:
            if Ctx.provider in database:
                yield Ctx

    db_name = 'testdb'


pony_fixtures.appendleft(DBContext.invoke)


class MySqlContext(DBContext):
    provider = 'mysql'


    def drop_db(self, cursor):
        cursor.execute('use sys')
        cursor.execute('drop database %s' % self.db_name)


    def init_db(self):
        with closing(mysql_module.connect(**self.CONN).cursor()) as c:
            try:
                self.drop_db(c)
            except mysql_module.DatabaseError as exc:
                print('Failed to drop db: %s' % exc)
            c.execute('create database %s' % self.db_name)
            c.execute('use %s' % self.db_name)

    CONN = {
        'host': "localhost",
        'user': "ponytest",
        'passwd': "ponytest",
    }

    @cached_property
    def db(self):
        CONN = dict(self.CONN, db=self.db_name)
        return Database('mysql', **CONN)


class SqlServerContext(DBContext):

    provider = 'sqlserver'

    def get_conn_string(self, db=None):
        s = (
            'DSN=MSSQLdb;'
            'SERVER=mssql;'
            'UID=sa;'
            'PWD=pass;'
        )
        if db:
            s += 'DATABASE=%s' % db
        return s

    @cached_property
    def db(self):
        CONN = self.get_conn_string(self.db_name)
        return Database('mssqlserver', CONN)

    def init_db(self):
        import pyodbc
        cursor = pyodbc.connect(self.get_conn_string(), autocommit=True).cursor()
        with closing(cursor) as c:
            try:
                self.drop_db(c)
            except pyodbc.DatabaseError as exc:
                print('Failed to drop db: %s' % exc)
            c.execute('create database %s' % self.db_name)
            c.execute('use %s' % self.db_name)

    def drop_db(self, cursor):
        cursor.execute('use master')
        cursor.execute('drop database %s' % self.db_name)


class SqliteContext(DBContext):
    provider = 'sqlite'

    def init_db(self):
        try:
            os.remove(self.db_path)
        except OSError as exc:
            print('Failed to drop db: %s' % exc)


    @cached_property
    def db_path(self):
        p = os.path.dirname(__file__)
        p = os.path.join(p, self.db_name)
        return os.path.abspath(p)

    @cached_property
    def db(self):
        return Database('sqlite', self.db_path, create_db=True)


class PostgresContext(DBContext):
    provider = 'postgresql'

    def get_conn_dict(self, no_db=False):
        d = dict(
            user='ponytest', password='ponytest',
            host='localhost'
        )
        if not no_db:
            d.update(database=self.db_name)
        return d

    def init_db(self):
        import psycopg2
        conn = psycopg2.connect(
            **self.get_conn_dict(no_db=True)
        )
        conn.set_isolation_level(0)
        with closing(conn.cursor()) as cursor:
            try:
                self.drop_db(cursor)
            except psycopg2.DatabaseError as exc:
                print('Failed to drop db: %s' % exc)
            cursor.execute('create database %s' % self.db_name)

    def drop_db(self, cursor):
        cursor.execute('drop database %s' % self.db_name)


    @cached_property
    def db(self):
        return Database('postgres', **self.get_conn_dict())


class OracleContext(DBContext):
    provider = 'oracle'

    def __enter__(self):
        os.environ.update(dict(
            ORACLE_BASE='/u01/app/oracle',
            ORACLE_HOME='/u01/app/oracle/product/12.1.0/dbhome_1',
            ORACLE_OWNR='oracle',
            ORACLE_SID='orcl',
        ))
        return super(OracleContext, self).__enter__()

    def init_db(self):
        import cx_Oracle
        with closing(self.connect_sys()) as conn:
            with closing(conn.cursor()) as cursor:
                try:
                    self._destroy_test_user(cursor)
                    self._drop_tablespace(cursor)
                except cx_Oracle.DatabaseError as exc:
                    print('Failed to drop db: %s' % exc)
                cursor.execute(
                """CREATE TABLESPACE %(tblspace)s
                DATAFILE '%(datafile)s' SIZE 20M
                REUSE AUTOEXTEND ON NEXT 10M MAXSIZE %(maxsize)s
                """ % self.parameters)
                cursor.execute(
                """CREATE TEMPORARY TABLESPACE %(tblspace_temp)s
                TEMPFILE '%(datafile_tmp)s' SIZE 20M
                REUSE AUTOEXTEND ON NEXT 10M MAXSIZE %(maxsize_tmp)s
                """ % self.parameters)
                self._create_test_user(cursor)


    def _drop_tablespace(self, cursor):
        cursor.execute(
            'DROP TABLESPACE %(tblspace)s INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS'
        % self.parameters)
        cursor.execute(
            'DROP TABLESPACE %(tblspace_temp)s INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS'
        % self.parameters)


    parameters = {
        'tblspace': 'test_tblspace',
        'tblspace_temp': 'test_tblspace_temp',
        'datafile': 'test_datafile.dbf',
        'datafile_tmp': 'test_datafile_tmp.dbf',
        'user': 'ponytest',
        'password': 'ponytest',
        'maxsize': '100M',
        'maxsize_tmp': '100M',
    }

    def connect_sys(self):
        import cx_Oracle
        return cx_Oracle.connect('sys/the@localhost/ORCL', mode=cx_Oracle.SYSDBA)

    def connect_test(self):
        import cx_Oracle
        return cx_Oracle.connect('test_user/test_password@localhost/ORCL')


    @cached_property
    def db(self):
        return Database('oracle', 'test_user/test_password@localhost/ORCL')

    def _create_test_user(self, cursor):
        cursor.execute(
        """CREATE USER %(user)s
            IDENTIFIED BY %(password)s
            DEFAULT TABLESPACE %(tblspace)s
            TEMPORARY TABLESPACE %(tblspace_temp)s
            QUOTA UNLIMITED ON %(tblspace)s
        """ % self.parameters
        )
        cursor.execute(
        """GRANT CREATE SESSION,
                    CREATE TABLE,
                    CREATE SEQUENCE,
                    CREATE PROCEDURE,
                    CREATE TRIGGER
            TO %(user)s
        """ % self.parameters
        )

    def _destroy_test_user(self, cursor):
        cursor.execute('''
            DROP USER %(user)s CASCADE
        ''' % self.parameters)


@contextmanager
def logging_context(test):
    level = logging.getLogger().level
    from pony.orm.core import debug, sql_debug
    logging.getLogger().setLevel(logging.INFO)
    sql_debug(True)
    yield
    logging.getLogger().setLevel(level)
    sql_debug(debug)


@with_cli_args
@click.option('--log', is_flag=True)
def use_logging(log):
    if log:
        yield logging_context

pony_fixtures.appendleft(use_logging)


class DBSession(object):

    def __init__(self, test):
        self.test = test

    @property
    def in_db_session(self):
        ret = getattr(self.test, 'in_db_session', True)
        method = getattr(self.test, self.test._testMethodName)
        return getattr(method, 'in_db_session', ret)

    def __enter__(self):
        rollback()
        if self.in_db_session:
            db_session.__enter__()

    def __exit__(self, *exc_info):
        rollback()
        if self.in_db_session:
            db_session.__exit__()

pony_fixtures.appendleft([DBSession])
