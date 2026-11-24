from __future__ import annotations

from operator import attrgetter
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, cast

from pony.orm import core
from pony.orm.core import DBSchemaError, MappingError, log_sql
from pony.py23compat import int_types
from pony.utils import throw

if TYPE_CHECKING:
    from collections.abc import Sequence

    import _typeshed
    from typing_extensions import Self

    from pony.orm.core import (
        Attribute,
        DbConnection,
        EntityMeta,
        TableName,
    )
    from pony.orm.dbapiprovider import Converter, DBAPIProvider


class DBSchema:
    provider: DBAPIProvider
    tables: dict[TableName, Table]
    constraints: dict[str, Constraint]
    indent: str
    command_separator: str
    uppercase: bool
    names: dict[TableName, Table]

    dialect: ClassVar[str | _typeshed.MaybeNone] = None
    inline_fk_syntax: ClassVar[bool] = True
    named_foreign_keys: ClassVar[bool] = True
    table_class: ClassVar[type[Table]]
    column_class: ClassVar[type[Column]]
    index_class: ClassVar[type[DBIndex]]
    fk_class: ClassVar[type[ForeignKey]]

    def __init__(self, provider: DBAPIProvider, uppercase: bool = True) -> None:
        self.provider = provider
        self.tables = {}
        self.constraints = {}
        self.indent = "  "
        self.command_separator = ";\n\n"
        self.uppercase = uppercase
        self.names = {}

    def column_list(self, columns: Sequence[Column]) -> str:
        quote_name = self.provider.quote_name
        return "(%s)" % ", ".join(quote_name(column.name) for column in columns)

    def case(self, s: str) -> str:
        if self.uppercase:
            return (
                s.upper()
                .replace("%S", "%s")
                .replace(")S", ")s")
                .replace("%R", "%r")
                .replace(")R", ")r")
            )
        else:
            return s.lower()

    def add_table(
        self, table_name: TableName, entity: EntityMeta | None = None
    ) -> Table:
        return self.table_class(table_name, self, entity)

    def order_tables_to_create(self) -> list[Table | Any]:
        tables = []
        created_tables: set[Table] = set()
        split = self.provider.split_table_name
        tables_to_create = sorted(
            self.tables.values(), key=lambda table: split(table.name)
        )
        while tables_to_create:
            for table in tables_to_create:
                if table.parent_tables.issubset(created_tables):
                    created_tables.add(table)
                    tables_to_create.remove(table)
                    break
            else:
                table = tables_to_create.pop()
            tables.append(table)
        return tables

    def generate_create_script(self) -> str:
        created_tables: set[Table] = set()
        commands = []
        for table in self.order_tables_to_create():
            for db_object in table.get_objects_to_create(created_tables):
                commands.append(db_object.get_create_command())
        return self.command_separator.join(commands)

    def create_tables(self, provider: DBAPIProvider, connection: DbConnection) -> None:
        created_tables: set[Table] = set()
        for table in self.order_tables_to_create():
            for db_object in table.get_objects_to_create(created_tables):
                base_name = provider.base_name(db_object.name)  # type: ignore[arg-type]
                name = db_object.exists(provider, connection, case_sensitive=False)
                if name is None:
                    db_object.create(provider, connection)
                elif name != base_name:
                    quote_name = self.provider.quote_name
                    n1, n2 = quote_name(db_object.name), quote_name(name)  # type: ignore[arg-type]
                    tn1, tn2 = db_object.typename, db_object.typename.lower()
                    throw(
                        DBSchemaError,
                        "%s %s cannot be created, because %s %s "
                        "(with a different letter case) already exists in the database. "
                        "Try to delete %s %s first." % (tn1, n1, tn2, n2, n2, tn2),
                    )

    def check_tables(self, provider: DBAPIProvider, connection: DbConnection) -> None:
        cursor = connection.cursor()
        split = provider.split_table_name
        for table in sorted(self.tables.values(), key=lambda table: split(table.name)):
            alias = provider.base_name(table.name)
            sql_ast = [
                "SELECT",
                [
                    "ALL",
                ]
                + [["COLUMN", alias, column.name] for column in table.column_list],
                ["FROM", [alias, "TABLE", table.name]],
                ["WHERE", ["EQ", ["VALUE", 0], ["VALUE", 1]]],
            ]
            sql, adapter = provider.ast2sql(sql_ast)
            if core.local.debug:
                log_sql(sql)
            provider.execute(cursor, sql)


class _DBObjectWithCreateCommand(Protocol):
    def get_create_command(self) -> str: ...


class DBObject:
    def create(self, provider: DBAPIProvider, connection: DbConnection) -> None:
        sql = cast("_DBObjectWithCreateCommand", self).get_create_command()
        if core.local.debug:
            log_sql(sql)
        cursor = connection.cursor()
        provider.execute(cursor, sql)


class Table(DBObject):
    typename = "Table"

    pk_index: DBIndex | None

    def __init__(
        self, name: TableName, schema: DBSchema, entity: EntityMeta | None = None
    ) -> None:
        if name in schema.tables:
            throw(
                DBSchemaError,
                "Table %r already exists in database schema" % name,  # type: ignore[str-format]
            )
        if name in schema.names:
            throw(
                DBSchemaError,
                "Table %r cannot be created, name is already in use" % name,  # type: ignore[str-format]
            )
        schema.tables[name] = self
        schema.names[name] = self
        self.schema = schema
        self.name = name
        self.column_list: list[Column] = []
        self.column_dict: dict[str, Column] = {}
        self.indexes: dict[tuple[Column, ...], DBIndex] = {}
        self.pk_index = None
        self.foreign_keys: dict[tuple[Column, ...], ForeignKey] = {}
        self.parent_tables: set[Table] = set()
        self.child_tables: set[Table] = set()
        self.entities: set[EntityMeta] = set()
        self.options: dict[str, object] = {}
        if entity is not None:
            self.entities.add(entity)
            self.options = entity._table_options_
        self.m2m: set[Attribute] = set()

    def __repr__(self):
        return "<Table(%s)>" % self.schema.provider.format_table_name(self.name)

    def add_entity(self, entity: EntityMeta) -> None:
        for e in self.entities:
            if e._root_ is not entity._root_:
                throw(
                    MappingError,
                    "Entities %s and %s cannot be mapped to table %s "
                    "because they don't belong to the same hierarchy"
                    % (e, entity, self.name),
                )
        assert "_table_options_" not in entity.__dict__
        self.entities.add(entity)

    def exists(
        self,
        provider: DBAPIProvider,
        connection: DbConnection,
        case_sensitive: bool = True,
    ) -> str | None:
        return provider.table_exists(connection, self.name, case_sensitive)

    def get_create_command(self) -> str:
        schema = self.schema
        case = schema.case
        provider = schema.provider
        quote_name = provider.quote_name
        if_not_exists = False  # provider.table_if_not_exists_syntax and provider.index_if_not_exists_syntax
        cmd = []
        if not if_not_exists:
            cmd.append(case("CREATE TABLE %s (") % quote_name(self.name))
        else:
            cmd.append(case("CREATE TABLE IF NOT EXISTS %s (") % quote_name(self.name))
        for column in self.column_list:
            cmd.append(schema.indent + column.get_sql() + ",")
        if self.pk_index and len(self.pk_index.columns) > 1:
            cmd.append(schema.indent + self.pk_index.get_sql() + ",")
        indexes = [
            index
            for index in self.indexes.values()
            if not index.is_pk and index.is_unique and len(index.columns) > 1
        ]
        for index in indexes:
            assert index.name is not None
        indexes.sort(key=attrgetter("name"))
        for index in indexes:
            cmd.append(schema.indent + index.get_sql() + ",")
        if not schema.named_foreign_keys:
            for foreign_key in sorted(
                self.foreign_keys.values(), key=lambda fk: fk.name or ""
            ):
                if schema.inline_fk_syntax and len(foreign_key.child_columns) == 1:
                    continue
                cmd.append(schema.indent + foreign_key.get_sql() + ",")
        interleave_fks = [fk for fk in self.foreign_keys.values() if fk.interleave]
        if interleave_fks:
            assert len(interleave_fks) == 1
            fk = interleave_fks[0]
            cmd.append(schema.indent + fk.get_sql())
            cmd.append(
                case(") INTERLEAVE IN PARENT %s (%s)")
                % (
                    quote_name(fk.parent_table.name),
                    ", ".join(quote_name(col.name) for col in fk.child_columns),
                )
            )
        else:
            cmd[-1] = cmd[-1][:-1]
            cmd.append(")")
        for name, value in sorted(self.options.items()):
            option = self.format_option(name, value)
            if option:
                cmd.append(option)
        return "\n".join(cmd)

    def format_option(self, name, value):
        if value is True:
            return name
        if value is False:
            return None
        return "%s %s" % (name, value)

    def get_objects_to_create(
        self, created_tables: set[Self] | None = None
    ) -> Sequence[Table | DBIndex | ForeignKey]:
        if created_tables is None:
            created_tables = set()
        created_tables.add(self)
        result: list[Self | DBIndex | ForeignKey] = [self]
        indexes = [
            index
            for index in self.indexes.values()
            if not index.is_pk and not index.is_unique
        ]
        for index in indexes:
            assert index.name is not None
        indexes.sort(key=attrgetter("name"))
        result.extend(indexes)
        schema = self.schema
        if schema.named_foreign_keys:
            for foreign_key in sorted(
                self.foreign_keys.values(), key=lambda fk: fk.name or ""
            ):
                if foreign_key.parent_table not in created_tables:
                    continue
                result.append(foreign_key)
            for child_table in self.child_tables:
                if child_table not in created_tables:
                    continue
                for foreign_key in sorted(
                    child_table.foreign_keys.values(), key=lambda fk: fk.name or ""
                ):
                    if foreign_key.parent_table is not self:
                        continue
                    result.append(foreign_key)
        return result

    def add_column(
        self,
        column_name: str,
        sql_type: str,
        converter: Converter,
        is_not_null: bool | None = None,
        sql_default: str | bool | None = None,
    ) -> Column:
        return self.schema.column_class(
            column_name, self, sql_type, converter, is_not_null, sql_default
        )

    def add_index(
        self,
        index_name: str | bool | None,
        columns: tuple[Column, ...],
        is_pk: bool | Literal["auto"] = False,
        is_unique: bool | None = None,
        m2m: bool = False,
    ) -> DBIndex:
        assert index_name is not False
        if index_name is True:
            index_name = None
        if index_name is None and not is_pk:
            provider = self.schema.provider
            index_name = provider.get_default_index_name(
                self.name,
                (column.name for column in columns),
                is_pk=is_pk,
                is_unique=is_unique,
                m2m=m2m,
            )
        index = self.indexes.get(columns)
        if (
            index
            and index.name == index_name
            and index.is_pk == is_pk
            and index.is_unique == is_unique
        ):
            return index
        return self.schema.index_class(index_name, self, columns, is_pk, is_unique)

    def add_foreign_key(
        self,
        fk_name: str | None,
        child_columns: tuple[Column, ...],
        parent_table: Self,
        parent_columns: tuple[Column, ...],
        index_name: str | bool | None = None,
        on_delete: str | bool | None = False,
        interleave: bool | None = False,
    ) -> ForeignKey:
        if fk_name is None:
            provider = self.schema.provider
            child_column_names = tuple(column.name for column in child_columns)
            fk_name = provider.get_default_fk_name(
                self.name, parent_table.name, child_column_names
            )
        return self.schema.fk_class(
            fk_name,
            self,
            child_columns,
            parent_table,
            parent_columns,
            index_name,
            on_delete,
            interleave=interleave,
        )


class Column:
    table: Table
    name: str
    sql_type: str
    converter: Converter
    is_not_null: bool | None
    sql_default: str | bool | None
    is_pk: bool | Literal["auto"]
    is_pk_part: bool
    is_unique: bool

    auto_template: ClassVar[str | None] = "%(type)s PRIMARY KEY AUTOINCREMENT"

    def __init__(
        self,
        name: str,
        table: Table,
        sql_type: str,
        converter: Converter,
        is_not_null: bool | None = None,
        sql_default: str | bool | None = None,
    ) -> None:
        if name in table.column_dict:
            throw(
                DBSchemaError,
                "Column %r already exists in table %r" % (name, table.name),
            )
        table.column_dict[name] = self
        table.column_list.append(self)
        self.table = table
        self.name = name
        self.sql_type = sql_type
        self.converter = converter
        self.is_not_null = is_not_null
        self.sql_default = sql_default
        self.is_pk = False
        self.is_pk_part = False
        self.is_unique = False

    def __repr__(self):
        return "<Column(%s.%s)>" % (self.table.name, self.name)

    def get_sql(self) -> str:
        table = self.table
        schema = table.schema
        quote_name = schema.provider.quote_name
        case = schema.case
        result: list[str] = []
        append = result.append
        append(quote_name(self.name))

        def add_default():
            if self.sql_default not in (None, True, False):
                append(case("DEFAULT"))
                append(self.sql_default)

        if (
            self.is_pk == "auto"
            and self.auto_template
            and self.converter.py_type in int_types
        ):
            append(case(self.auto_template % dict(type=self.sql_type)))
            add_default()
        else:
            append(case(self.sql_type))
            add_default()
            if self.is_pk:
                if schema.dialect == "SQLite":
                    append(case("NOT NULL"))
                append(case("PRIMARY KEY"))
            else:
                if self.is_unique:
                    append(case("UNIQUE"))
                if self.is_not_null:
                    append(case("NOT NULL"))
        if schema.inline_fk_syntax and not schema.named_foreign_keys:
            foreign_key = table.foreign_keys.get((self,))
            if foreign_key is not None:
                parent_table = foreign_key.parent_table
                append(case("REFERENCES"))
                append(quote_name(parent_table.name))
                append(schema.column_list(foreign_key.parent_columns))
                if foreign_key.on_delete:
                    append("ON DELETE %s" % foreign_key.on_delete)
        return " ".join(result)


class Constraint(DBObject):
    def __init__(self, name: str | None, schema: DBSchema) -> None:
        if name is not None:
            assert name not in schema.names
            if name in schema.constraints:
                throw(DBSchemaError, "Constraint with name %r already exists" % name)
            schema.names[name] = self  # type: ignore[assignment]
            schema.constraints[name] = self
        self.schema = schema
        self.name = name


class DBIndex(Constraint):
    typename: ClassVar[str] = "Index"

    def __init__(
        self,
        name: str | None,
        table: Table,
        columns: tuple[Column, ...],
        is_pk: bool | Literal["auto"] = False,
        is_unique: bool | None = None,
    ) -> None:
        assert len(columns) > 0
        for column in columns:
            if column.table is not table:
                throw(
                    DBSchemaError,
                    "Column %r does not belong to table %r and cannot be part of its index"
                    % (column.name, table.name),
                )
        if columns in table.indexes:
            if len(columns) == 1:
                throw(
                    DBSchemaError,
                    "Index for column %r already exists" % columns[0].name,
                )
            else:
                throw(
                    DBSchemaError,
                    "Index for columns (%s) already exists"
                    % ", ".join(repr(column.name) for column in columns),
                )
        if is_pk:
            if table.pk_index is not None:
                throw(
                    DBSchemaError,
                    "Primary key for table %r is already defined" % table.name,  # type: ignore[str-format]
                )
            table.pk_index = self
            if is_unique is None:
                is_unique = True
            elif not is_unique:
                throw(
                    DBSchemaError,
                    "Incompatible combination of is_unique=False and is_pk=True",
                )
        elif is_unique is None:
            is_unique = False
        schema = table.schema
        if name is not None and name in schema.names:
            throw(
                DBSchemaError,
                "Index %s cannot be created, name is already in use" % name,
            )
        Constraint.__init__(self, name, schema)
        for column in columns:
            column.is_pk = column.is_pk or (len(columns) == 1 and is_pk)
            column.is_pk_part = column.is_pk_part or bool(is_pk)
            column.is_unique = column.is_unique or (is_unique and len(columns) == 1)
        table.indexes[columns] = self
        self.table = table
        self.columns = columns
        self.is_pk = is_pk
        self.is_unique = is_unique

    def exists(
        self,
        provider: DBAPIProvider,
        connection: DbConnection,
        case_sensitive: bool = True,
    ) -> str | None:
        return provider.index_exists(
            connection,
            self.table.name,
            self.name,  # type: ignore[arg-type]
            case_sensitive,
        )

    def get_sql(self) -> str:
        return self._get_create_sql(inside_table=True)

    def get_create_command(self) -> str:
        return self._get_create_sql(inside_table=False)

    def _get_create_sql(self, inside_table: bool) -> str:
        schema = self.schema
        case = schema.case
        quote_name = schema.provider.quote_name
        cmd: list[str] = []
        append = cmd.append
        if not inside_table:
            if self.is_pk:
                throw(
                    DBSchemaError,
                    "Primary key index cannot be defined outside of table definition",
                )
            append(case("CREATE"))
            if self.is_unique:
                append(case("UNIQUE"))
            append(case("INDEX"))
            # if schema.provider.index_if_not_exists_syntax:
            #     append(case('IF NOT EXISTS'))
            append(quote_name(self.name))  # type: ignore[arg-type]
            append(case("ON"))
            append(quote_name(self.table.name))
            converter = self.columns[0].converter
            if (
                isinstance(converter.py_type, core.Array)
                and converter.provider.dialect == "PostgreSQL"
            ):
                append(case("USING GIN"))
        else:
            if self.name:
                append(case("CONSTRAINT"))
                append(quote_name(self.name))
            if self.is_pk:
                append(case("PRIMARY KEY"))
            elif self.is_unique:
                append(case("UNIQUE"))
            else:
                append(case("INDEX"))
        append(schema.column_list(self.columns))
        return " ".join(cmd)


class ForeignKey(Constraint):
    typename: ClassVar[str] = "Foreign key"

    def __init__(
        self,
        name: str,
        child_table: Table,
        child_columns: tuple[Column, ...],
        parent_table: Table,
        parent_columns: tuple[Column, ...],
        index_name: str | bool | None,
        on_delete: str | bool | None,
        interleave: bool | None = False,
    ) -> None:
        schema = parent_table.schema
        if schema is not child_table.schema:
            throw(
                DBSchemaError,
                "Parent and child tables of foreign_key cannot belong to different schemata",
            )
        for column in parent_columns:
            if column.table is not parent_table:
                throw(
                    DBSchemaError,
                    "Column %r does not belong to table %r"
                    % (column.name, parent_table.name),
                )
        for column in child_columns:
            if column.table is not child_table:
                throw(
                    DBSchemaError,
                    "Column %r does not belong to table %r"
                    % (column.name, child_table.name),
                )
        if len(parent_columns) != len(child_columns):
            throw(DBSchemaError, "Foreign key columns count do not match")
        if child_columns in child_table.foreign_keys:
            if len(child_columns) == 1:
                throw(
                    DBSchemaError,
                    "Foreign key for column %r already defined" % child_columns[0].name,
                )
            else:
                throw(
                    DBSchemaError,
                    "Foreign key for columns (%s) already defined"
                    % ", ".join(repr(column.name) for column in child_columns),
                )
        if name is not None and name in schema.names:
            throw(
                DBSchemaError,
                "Foreign key %s cannot be created, name is already in use" % name,
            )
        Constraint.__init__(self, name, schema)
        child_table.foreign_keys[child_columns] = self
        if child_table is not parent_table:
            child_table.parent_tables.add(parent_table)
            parent_table.child_tables.add(child_table)
        self.parent_table = parent_table
        self.parent_columns = parent_columns
        self.child_table = child_table
        self.child_columns = child_columns
        self.on_delete = on_delete
        self.interleave = interleave

        if index_name is not False:
            child_columns_len = len(child_columns)
            if all(
                columns[:child_columns_len] != child_columns
                for columns in child_table.indexes
            ):
                child_table.add_index(
                    index_name,
                    child_columns,
                    is_pk=False,
                    is_unique=False,
                    m2m=bool(child_table.m2m),
                )

    def exists(self, provider, connection, case_sensitive=True):
        return provider.fk_exists(
            connection, self.child_table.name, self.name, case_sensitive
        )

    def get_sql(self) -> str:
        return self._get_create_sql(inside_table=True)

    def get_create_command(self) -> str:
        return self._get_create_sql(inside_table=False)

    def _get_create_sql(self, inside_table: bool) -> str:
        schema = self.schema
        case = schema.case
        quote_name = schema.provider.quote_name
        cmd: list[str] = []
        append = cmd.append
        if not inside_table:
            append(case("ALTER TABLE"))
            append(quote_name(self.child_table.name))
            append(case("ADD"))
        if schema.named_foreign_keys and self.name:
            append(case("CONSTRAINT"))
            append(quote_name(self.name))
        append(case("FOREIGN KEY"))
        append(schema.column_list(self.child_columns))
        append(case("REFERENCES"))
        append(quote_name(self.parent_table.name))
        append(schema.column_list(self.parent_columns))
        if self.on_delete:
            append(case("ON DELETE %s" % self.on_delete))
        return " ".join(cmd)


DBSchema.table_class = Table
DBSchema.column_class = Column
DBSchema.index_class = DBIndex
DBSchema.fk_class = ForeignKey
