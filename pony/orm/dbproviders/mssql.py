# Module  : PonyORM MSSql Driver for SQLServer 2005+
# Author  : lijiajie@chiahddz.com
# Date    : 2013-12-27
# Version : 1.0


import pymssql as mssql  # @UnresolvedImport

from pony.orm.dbapiprovider import DBAPIProvider, Pool, ProgrammingError
from pony.orm import dbschema, dbapiprovider
from pony.orm.sqltranslation import SQLTranslator
from pony.orm.sqlbuilding import SQLBuilder
from decimal import Decimal
from datetime import date, datetime
from uuid import UUID


DIALECT = 'MSSQL'

class MSSQLColumn(dbschema.Column):
    auto_template = '%(type)s PRIMARY KEY IDENTITY(1,1)'

class MSSQLSchema(dbschema.DBSchema):
    dialect = DIALECT
    column_class = MSSQLColumn
    
class MSSQLTranslator(SQLTranslator):
    dialect = DIALECT

class MSSQLBuilder(SQLBuilder):
    
    dialect = DIALECT
    
    def SELECT(self, *sections):
        
        last_section = sections[-1]
        limit = offset = None
        
        if last_section[0] == 'LIMIT':
            limit = last_section[1]
            if len(last_section) > 2: offset = last_section[2]
            sections = sections[:-1]
            
        result = self.subquery(*sections)
        indent = self.indent_spaces * self.indent
        if not limit: pass
        elif not offset:
            
            result = [ 'SELECT TOP ', self(limit) ,' t.* FROM (\n' ]
            self.indent += 1
            result.extend(self.subquery(*sections))
            self.indent -= 1
            result.extend((indent, ') t'))
        else:
            
            last_section = sections[-1]
            if last_section[0] is not 'ORDER_BY': raise ProgrammingError("No ORDER_BY page function");
            sections = sections[:-1]
            
            for v in last_section:
                if not isinstance ( v , list): continue
                if len(v) is 2:
                    if v[0] is 'DESC' or v[0] is 'ASC' and isinstance(v[1],list) and v[1][0] == 'COLUMN' and len(v[1]) == 3:
                        v[1][1] = 't';
                elif len(v) is 3 and v[0] == 'COLUMN':
                    v[1] = 't';

            indent2 = indent + self.indent_spaces
            result = [ 'SELECT * FROM (\n', indent2, 'SELECT t.*, ROW_NUMBER() OVER(' ]
            result.extend(self(last_section)) 
            result.extend(') as "row-num" FROM (\n' )
            self.indent += 2
            result.extend(self.subquery(*sections))
            self.indent -= 2
            result.extend((indent2, ') t '))
            result.extend((indent, ') p WHERE "row-num" > ', self(offset)))
            if limit[0] == 'VALUE' and offset[0] == 'VALUE' \
                    and isinstance(limit[1], int) and isinstance(offset[1], int):
                total_limit = [ 'VALUE', limit[1] + offset[1] ]
                result.extend((' and "row-num" <= ', self(total_limit), '\n'))
            else: result.extend((' and "row-num" <= ', self(limit), ' + ', self(offset), '\n'))
        if self.indent:
            indent = self.indent_spaces * self.indent
            return '(\n', result, indent + ')'
        return result
    
class MSSQLBooleanConverter(dbapiprovider.BoolConverter):
    def sql_type(self):
        return "BIT"

def _string_sql_type(converter):
        if converter.max_len:
            return 'NVARCHAR(%d)' % converter.max_len
        return 'TEXT'
class MSSQLBasestringConverter(dbapiprovider.BasestringConverter):
    
    sql_type = _string_sql_type

class MSSQLStrConverter(dbapiprovider.StrConverter):
    
    sql_type = _string_sql_type
    
class MSSQLUnicodeConverter(dbapiprovider.UnicodeConverter):
    
    sql_type = _string_sql_type
    
class MSSQLProvider(DBAPIProvider):
  
    dialect = DIALECT
    quote_char='"'
    max_name_len = 128
    select_for_update_nowait_syntax = False
    
    default_schema_name = 'dbo'

    dbapi_module = mssql
    dbschema_cls = MSSQLSchema
    translator_cls = MSSQLTranslator
    sqlbuilder_cls = MSSQLBuilder

    name_before_table = 'db_name'
    paramstyle='format'

    converter_classes = [
        (bool, MSSQLBooleanConverter),
        (unicode, MSSQLUnicodeConverter),
        (str, MSSQLStrConverter),
        ((int, long), dbapiprovider.IntConverter),
        (float, dbapiprovider.RealConverter),
        (Decimal, dbapiprovider.DecimalConverter),
        (buffer, dbapiprovider.BlobConverter),
        (datetime, dbapiprovider.DatetimeConverter),
        (date, dbapiprovider.DatetimeConverter),
        (UUID, dbapiprovider.UuidConverter),
    ]

    def get_pool(self, *args, **kwargs):
        return Pool(mssql, *args, **kwargs)

    def table_exists(self, connection, table_name):
        
        db_name, table_name = self.split_table_name(table_name)
        
        sql  = "SELECT top 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME = %s"
        cursor = connection.cursor()
        cursor.execute(sql, tuple([ db_name, table_name ]))
        
        return cursor.fetchone() is not None
    
    def index_exists(self, connection, table_name, index_name):
        
        db_name, table_name = self.split_table_name(table_name)
        
        sql  = "SELECT top 1 1 FROM sys.indexes WHERE NAME=%s AND object_id = OBJECT_ID(%s)"
        cursor = connection.cursor()
        cursor.execute(sql, tuple([ db_name, table_name ]))
        
        return cursor.fetchone() is not None

    def fk_exists(self, connection, table_name, fk_name):
      
        sql = "SELECT 1 FROM sys.foreign_keys WHERE object_id = OBJECT_ID(%s) AND parent_object_id=%s";
        cursor = connection.cursor()
        cursor.execute(sql, tuple([ table_name, fk_name ]))
        
        return cursor.fetchone() is not None

    def disable_fk_checks_if_necessary(self, connection):
        
        return False

    def enable_fk_checks_if_necessary(self, connection, fk):
        
        return False
    
    def drop_table(self, connection, table_name):
        
        cursor = connection.cursor()
        
        sql = '''
            
            DECLARE @sql nvarchar(1000)
            
            WHILE EXISTS(
                SELECT * 
                FROM sys.foreign_keys
                WHERE referenced_object_id = object_id('%(table)s')
            )
            BEGIN
                SELECT 
                    @sql = 'ALTER TABLE ' +  OBJECT_SCHEMA_NAME(parent_object_id) +
                    '.[' + OBJECT_NAME(parent_object_id) + 
                    '] DROP CONSTRAINT ' + name
                    FROM sys.foreign_keys
                    WHERE referenced_object_id = object_id('%(table)s')
                exec  sp_executesql @sql
            END
            
            DROP TABLE "%(table)s"
            
        ''' % { 'table' : table_name }
        
        cursor.execute(sql)

provider_cls = MSSQLProvider
