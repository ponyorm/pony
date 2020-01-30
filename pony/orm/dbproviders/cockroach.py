from __future__ import absolute_import
from pony.py23compat import PY2, basestring, unicode, buffer, int_types

from decimal import Decimal
from datetime import datetime, date, time, timedelta
from uuid import UUID

try:
    import psycopg2
except ImportError:
    try:
        from psycopg2cffi import compat
    except ImportError:
        raise ImportError('In order to use PonyORM with CockroachDB please install psycopg2 or psycopg2cffi')
    else:
        compat.register()

from pony.orm.dbproviders.postgres import (
    PGSQLBuilder, PGColumn, PGSchema, PGTranslator, PGProvider,
    PGStrConverter, PGIntConverter, PGRealConverter,
    PGDatetimeConverter, PGTimedeltaConverter,
    PGBlobConverter, PGJsonConverter, PGArrayConverter,
)

from pony.orm import core, dbapiprovider, ormtypes
from pony.orm.core import log_orm
from pony.orm.dbapiprovider import wrap_dbapi_exceptions

NoneType = type(None)

class CRColumn(PGColumn):
    auto_template = 'SERIAL PRIMARY KEY'

class CRSchema(PGSchema):
    column_class = CRColumn

class CRTranslator(PGTranslator):
    pass

class CRSQLBuilder(PGSQLBuilder):
    pass

class CRIntConverter(PGIntConverter):
    signed_types = {None: 'INT', 8: 'INT2', 16: 'INT2', 24: 'INT8', 32: 'INT8', 64: 'INT8'}
    unsigned_types = {None: 'INT', 8: 'INT2', 16: 'INT4', 24: 'INT8', 32: 'INT8'}
    # signed_types = {None: 'INT', 8: 'INT2', 16: 'INT2', 24: 'INT4', 32: 'INT4', 64: 'INT8'}
    # unsigned_types = {None: 'INT', 8: 'INT2', 16: 'INT4', 24: 'INT4', 32: 'INT8'}

class CRBlobConverter(PGBlobConverter):
    def sql_type(converter):
        return 'BYTES'

class CRTimedeltaConverter(PGTimedeltaConverter):
    sql_type_name = 'INTERVAL'

class PGUuidConverter(dbapiprovider.UuidConverter):
    def py2sql(converter, val):
        return val

class CRArrayConverter(PGArrayConverter):
    array_types = {
        int: ('INT', PGIntConverter),
        unicode: ('STRING', PGStrConverter),
        float: ('DOUBLE PRECISION', PGRealConverter)
    }

class CRProvider(PGProvider):
    dbapi_module = psycopg2
    dbschema_cls = CRSchema
    translator_cls = CRTranslator
    sqlbuilder_cls = CRSQLBuilder
    array_converter_cls = CRArrayConverter

    default_schema_name = 'public'

    fk_types = { 'SERIAL' : 'INT8' }

    def normalize_name(provider, name):
        return name[:provider.max_name_len].lower()

    @wrap_dbapi_exceptions
    def set_transaction_mode(provider, connection, cache):
        assert not cache.in_transaction
        db_session = cache.db_session
        if db_session is not None and db_session.ddl:
            cache.immediate = False
        if cache.immediate and connection.autocommit:
            connection.autocommit = False
            if core.local.debug: log_orm('SWITCH FROM AUTOCOMMIT TO TRANSACTION MODE')
        elif not cache.immediate and not connection.autocommit:
            connection.autocommit = True
            if core.local.debug: log_orm('SWITCH TO AUTOCOMMIT MODE')
        if db_session is not None and (db_session.serializable or db_session.ddl):
            cache.in_transaction = True

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (basestring, PGStrConverter),
        (int_types, CRIntConverter),
        (float, PGRealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (datetime, PGDatetimeConverter),
        (date, dbapiprovider.DateConverter),
        (time, dbapiprovider.TimeConverter),
        (timedelta, CRTimedeltaConverter),
        (UUID, PGUuidConverter),
        (buffer, CRBlobConverter),
        (ormtypes.Json, PGJsonConverter),
    ]

provider_cls = CRProvider
