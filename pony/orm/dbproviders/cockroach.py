from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, ClassVar
from uuid import UUID

from pony.orm import core, dbapiprovider, ormtypes
from pony.orm.core import log_orm
from pony.orm.dbapiprovider import wrap_dbapi_exceptions
from pony.orm.dbproviders.postgres import (
    PGArrayConverter,
    PGBlobConverter,
    PGColumn,
    PGDatetimeConverter,
    PGIntConverter,
    PGJsonConverter,
    PGProvider,
    PGRealConverter,
    PGSchema,
    PGSQLBuilder,
    PGTimedeltaConverter,
    PGTranslator,
)
from pony.py23compat import buffer, int_types

if TYPE_CHECKING:
    from typing_extensions import LiteralString

try:
    import psycopg2
except ImportError:
    try:
        from psycopg2cffi import compat
    except ImportError:
        raise ImportError(
            "In order to use PonyORM with CockroachDB please install psycopg2 or psycopg2cffi"
        )
    else:
        compat.register()

NoneType = type(None)


class CRColumn(PGColumn):
    auto_template: ClassVar[str | None] = "SERIAL PRIMARY KEY"


class CRSchema(PGSchema):
    column_class = CRColumn


class CRTranslator(PGTranslator):
    pass


class CRSQLBuilder(PGSQLBuilder):
    pass


class CRIntConverter(PGIntConverter):
    signed_types: dict[int | None, str] = {
        None: "INT",
        8: "INT2",
        16: "INT2",
        24: "INT8",
        32: "INT8",
        64: "INT8",
    }
    unsigned_types: dict[int | None, str] | None = {
        None: "INT",
        8: "INT2",
        16: "INT4",
        24: "INT8",
        32: "INT8",
    }
    # signed_types = {None: 'INT', 8: 'INT2', 16: 'INT2', 24: 'INT4', 32: 'INT4', 64: 'INT8'}
    # unsigned_types = {None: 'INT', 8: 'INT2', 16: 'INT4', 24: 'INT4', 32: 'INT8'}


class CRBlobConverter(PGBlobConverter):
    def sql_type(self) -> str:
        return "BYTES"


class CRTimedeltaConverter(PGTimedeltaConverter):
    sql_type_name: LiteralString = "INTERVAL"


class PGUuidConverter(dbapiprovider.UuidConverter):
    def py2sql(self, val):
        return val


class CRArrayConverter(PGArrayConverter):
    array_types = {
        int: ("INT", PGIntConverter),
        str: ("STRING", dbapiprovider.StrConverter),
        float: ("DOUBLE PRECISION", PGRealConverter),
    }


class CRProvider(PGProvider):
    dbapi_module = psycopg2  # pyright: ignore[reportPossiblyUnboundVariable]
    dbschema_cls = CRSchema
    translator_cls = CRTranslator
    sqlbuilder_cls = CRSQLBuilder
    array_converter_cls = CRArrayConverter

    default_schema_name = "public"

    fk_types = {"SERIAL": "INT8"}

    def normalize_name(self, name):
        return name[: self.max_name_len].lower()

    @wrap_dbapi_exceptions
    def set_transaction_mode(self, connection, cache):
        assert not cache.in_transaction
        db_session = cache.db_session
        if db_session is not None and db_session.ddl:
            cache.immediate = False
        if cache.immediate and connection.autocommit:
            connection.autocommit = False
            if core.local.debug:
                log_orm("SWITCH FROM AUTOCOMMIT TO TRANSACTION MODE")
        elif not cache.immediate and not connection.autocommit:
            connection.autocommit = True
            if core.local.debug:
                log_orm("SWITCH TO AUTOCOMMIT MODE")
        if db_session is not None and (db_session.serializable or db_session.ddl):
            cache.in_transaction = True

    converter_classes = [
        (NoneType, dbapiprovider.NoneConverter),
        (bool, dbapiprovider.BoolConverter),
        (str, dbapiprovider.StrConverter),
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
