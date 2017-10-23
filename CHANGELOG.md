# Pony ORM Release 0.7.3 (2017-10-23)

## New features

* `where()` method added to query
* `coalesce()` function added
* `between(x, a, b)` function added
* #295: Add `_table_options_` for entity class to specify engine, tablespace, etc.
* Make debug flag thread-local
* `sql_debugging` context manager added
* `sql_debug` and show_values arguments to db_session added
* `set_sql_debug` function added as alias to (to be deprecated) `sql_debug` function
* Allow `db_session` to accept `ddl` parameter when used as context manager
* Add `optimistic=True` option to db_session
* Skip optimistic checks for queries in `db_session` with `serializable=True`
* `fk_name` option added for attributes in order to specify foreign key name
* #280: Now it's possible to specify `timeout` option, as well as pass other keyword arguments for `sqlite3.connect` function
* Add support of explicit casting to int in queries using `int()` function
* Added modulo division % native support in queries

## Bugfixes

* Fix bugs with composite table names
* Fix invalid foreign key & index names for tables which names include schema name
* For queries like `select(x for x in MyObject if not x.description)` add "OR x.info IS NULL" for nullable string columns
* Add optimistic checking for `delete()` method
* Show updated attributes when `OptimisticCheckError` is being raised
* Fix incorrect aliases in nested queries
* Correctly pass exception from user-defined functions in SQLite
* More clear error messages for `UnrepeatableReadError`
* Fix `db_session(strict=True)` which was broken in 2d3afb24
* Fixes #170: Problem with a primary key column used as a part of another key
* Fixes #223: incorrect result of `getattr(entity, attrname)` when the same lambda applies to different entities
* Fixes #266: Add handler to `"pony.orm"` logger does not work
* Fixes #278: Cascade delete error: FOREIGN KEY constraint failed, with complex entity relationships
* Fixes #283: Lost Json update immediately after object creation
* Fixes #284: `query.order_by()` orders Json numbers like strings
* Fixes #288: Expression text parsing issue in Python 3
* Fixes #293: translation of if-expressions in expression 
* Fixes #294: Real stack traces swallowed within IPython shell
* `Collection.count()` method should check if session is alive
* Set `obj._session_cache_` to None after exiting from db session for better garbage collection
* Unload collections which are not fully loaded after exiting from db session for better garbage collection
* Raise on unknown options for attributes that are part of relationship


# Pony ORM Release 0.7.2 (2017-07-17)

## New features

* All arguments of db.bind() can be specified as keyword arguments. Previously Pony required the first positional argument which specified the database provider. Now you can pass all the database parameters using the dict: db.bind(**db_params). See https://docs.ponyorm.com/api_reference.html#Database.bind
* The `optimistic` attribute option is added https://docs.ponyorm.com/api_reference.html#cmdoption-arg-optimistic

## Bugfixes

* Fixes #219: when a database driver raises an error, sometimes this error was masked by the 'RollbackException: InterfaceError: connection already closed' exception. This happened because on error, Pony tried to rollback transaction, but the connection to the database was already closed and it masked the initial error. Now Pony displays the original error which helps to understand the cause of the problem.
* Fixes #276: Memory leak
* Fixes the __all__ declaration. Previously IDEs, such as PyCharm, could not understand what is going to be imported by 'from pony.orm import *'. Now it works fine.
* Fixes #232: negate check for numeric expressions now checks if value is zero or NULL
* Fixes #238, fixes #133: raise TransactionIntegrityError exception instead of AssertionError if obj.collection.create(**kwargs) creates a duplicate object
* Fixes #221: issue with unicode json path keys
* Fixes bug when discriminator column is used as a part of a primary key
* Handle situation when SQLite blob column contains non-binary value


# Pony ORM Release 0.7.1 (2017-01-10)

## New features

* New warning DatabaseContainsIncorrectEmptyValue added, it is raised when the required attribute is empty during loading an entity from the database

## Bugfixes

* Fixes #216: Added Python 3.6 support
* Fixes #203: subtranslator should use argnames from parent translator
* Change a way aliases in SQL query are generated in order to fix a problem when a subquery alias masks a base query alias
* Volatile attribute bug fixed
* Fix creation of self-referenced foreign keys - before this Pony didn't create the foreign key for self-referenced attributes
* Bug fixed: when required attribute is empty the loading from the database shouldn't raise the validation error. Now Pony raises the warning DatabaseContainsIncorrectEmptyValue
* Throw an error with more clear explanation when a list comprehension is used inside a query instead of a generator expression: "Use generator expression (... for ... in ...) instead of list comprehension [... for ... in ...] inside query"


# Pony ORM Release 0.7 (2016-10-11)

Starting with this release Pony ORM is release under the Apache License, Version 2.0.

## New features

* Added getattr() support in queries: https://docs.ponyorm.com/api_reference.html#getattr

## Backward incompatible changes

* #159: exceptions happened during flush() should not be wrapped with CommitException

Before this release an exception that happened in a hook(https://docs.ponyorm.com/api_reference.html#entity-hooks), could be raised in two ways - either wrapped into the CommitException or without wrapping. It depended if the exception happened during the execution of flush() or commit() function on the db_session exit. Now the exception happened inside the hook never will be wrapped into the CommitException.

## Bugfixes

* #190: Timedelta is not supported when using pymysql


# Pony ORM Release 0.6.6 (2016-08-22)

## New features

* Added native JSON data type support in all supported databases: https://docs.ponyorm.com/json.html

## Backward incompatible changes

* Dropped Python 2.6 support

## Improvements

* #179 Added the compatibility with PYPY using psycopg2cffi
* Added an experimental @db_session `strict` parameter: https://docs.ponyorm.com/transactions.html#strict

## Bugfixes

* #182 - LEFT JOIN doesn't work as expected for inherited entities when foreign key is None
* Some small bugs were fixed


# Pony ORM Release 0.6.5 (2016-04-04)

## Improvements

* Fixes #172: Query prefetch() method should load specified lazy attributes right in the main query if possible

## Bugfixes

* Fixes #168: Incorrect caching when slicing the same query multiple times
* Fixes #169: When py_check() returns False, Pony should truncate too large values in resulting ValueError message
* Fixes #171: AssertionError when saving changes of multiple objects
* Fixes #176: Autostripped strings are not validated correctly for Required attributes

See blog post for more detailed information: https://blog.ponyorm.com/2016/04/04/pony-orm-release-0-6-5/


# Pony ORM Release 0.6.4 (2016-02-10)

This release brings no new features, has no backward incompatible changes, only bug fixes.
If you are using obj.flush() method in your code we recommend you to upgrade to 0.6.4 release.

## Bugfixes

* #161: 0.6.3 + obj.flush(): after_insert, after_update & after_delete hooks do not work

# Pony ORM Release 0.6.3 (2016-02-05)

This release was intended to fix the behavior of obj.flush(), but failed to do it in a proper way.
Please skip this release and update to 0.6.4 if you are using obj.flush() method.

## Bugfixes

* Fixes #138 Incorrect behavior of obj.flush(): assertion failed after exception
* Fixes #157 Incorrect transaction state after obj.flush() caused "release unlocked lock" error in SQLite
* Fixes #151 SQLite + upper() or lower() does not work as expected


# Pony ORM Release 0.6.2 (2016-01-11)

The documentation was moved from this repo to a separate one at https://github.com/ponyorm/pony-doc
The compiled version can be found at https://docs.ponyorm.com

## New features

* Python 3.5 support
* #132, #145: raw_sql() function was added
* #126: Ability to use @db_session with generator functions
* #116: Add support to select by UUID
* Ability to get string SQL statement using the Query.get_sql() method
* New function delete(gen) and Query.delete(bulk=False)
* Now it is possible to override Entity.__init__() and declare custom entity methods

## Backward incompatible changes

* Normalizing table names for symmetric relationships
* Autostrip - automatically remove leading and trailing characters

## Bugfixes

* #87: Pony fails with pymysql installed as MySQLdb
* #118: Pony should reconnect if previous connection was created before process was forked
* #121: Unable to update value of unique attribute
* #122: AssertionError when changing part of a composite key
* #127: a workaround for incorrect pysqlite locking behavior
* #136: Cascade delete does not work correctly for one-to-one relationships
* #141, #143: remove restriction on adding new methods to entities
* #142: Entity.select_random() AssertionError
* #147: Add 'atom_expr' symbol handling for Python 3.5 grammar


# Pony ORM Release 0.6.1 (2015-02-20)

* Closed #65: Now the select(), filter(), order_by(), page(), limit(), random() methods can be applied to collection attributes
* Closed #105: Now you can pass globals and locals to the select() function
* Improved inheritance support in queries: select(x for x in BaseClass if x.subclass_attr == y)
* Now it is possible to do db.insert(SomeEntity, column1=x, column2=y) instead of db.insert(SomeEntity._table_, column1=x, column2=y)
* Discriminator attribute can be part of the composite index
* Now it is possible to specify the attribute name instead of the attribute itself in composite index
* Query statistics: global_stats_lock is deprecated, just use global_stats property without any locking
* New load() method for entity instances which retrieves all unloaded attributes except collections
* New load() method for collections, e.g. customer.orders.load()
* Enhanced error message when descendant classes declare attributes with the same name
* Fixed #98: Composite index can include attributes of base entity
* Fixed #106: incorrect loading of object which consists of primary key only
* Fixed pony.converting.check_email()
* Prefetching bug fixed: if collection is already fully loaded it shouldn't be loaded again
* Deprecated Entity.order_by(..) method was removed. Use Entity.select().order_by(...) instead
* Various performance enhancements
* Multiple bugs were fixed


# Pony ORM Release 0.6 (2014-11-05)

* Fixed #94: Aggregated subquery bug fixed

# Pony ORM Release Candidate 0.6rc3 (2014-10-30)

## Bugfixes

* Fixed #18: Allow to specify `size` and `unsigned` for int type
* Fixed #77: Discriminate Pony-generated fields in entities: Attribute.is_implicit field added
* Fixed #83: Entity.get() should issue LIMIT 2 when non-unique criteria used for search
* Fixed #84: executing db.insert() should turn off autocommit and begin transaction
* Fixed #88: composite_index(*attrs) added to support non-unique composite indexes
* Fixed #89: IN / NOT IN clauses works different with empty sequence
* Fixed #90: Do not automatically add "distinct" if query.first() used
* Fixed #91: document automatic "distinct" behaviour and also .without_distinct()
* Fixed #92: without_distinct() and first() do not work together correctly

## New features

* `size` and `unsigned` options for `int` attributes [`link`](http://doc.ponyorm.com/entities.html#max-integer-number-size)

Since the `long` type has gone in Python 3, the `long` type is deprecated in Pony now. Instead of `long` you should use the `int` type and specify the `size` option:

```python
    class MyEntity(db.Entity):
        attr1 = Required(long) # deprecated
        attr2 = Required(int, size=64) # new way for using BIGINT type in the database
```

# Pony ORM Release Candidate 0.6rc2 (2014-10-10)

## Bugfixes

* Fixes #81: python3.3: syntax error during installation in ubuntu 14.04


# Pony ORM Release Candidate 0.6rc1 (2014-10-08)

## New features:

* Python 3 support
* pymysql adapter support for MySQL databases

## Backward incompatible changes

Now Pony treats both `str` and `unicode` attribute types as they are unicode strings in both Python 2 and 3. So, the attribute declaration `attr = Required(str)` is equal to `attr = Required(unicode)` in Python 2 and 3. The same thing is with `LongStr` and `LongUnicode` - both of them are represented as unicode strings now.

For the sake of backward compatibility Pony adds `unicode` as an alias to `str` and `buffer` as an alias to `bytes` in Python 3.

## Other changes and bug fixes

* Fixes #74: Wrong FK column type when using sql_type on foreign ID column
* Fixes #75: MappingError for self-referenced entities in a many-to-many relationship
* Fixes #80: “Entity NoneType does not belong to database” when using to_dict


# Pony ORM Release 0.5.4 (2014-09-22)

## New functions and methods:

* `pony.orm.serialization` module with the `to_dict()` and `to_json()` functions was added. Before this release you could use the `to_dict()` method of an entity instance in order to get a key-value dictionary structure for a specific entity instance. Sometimes you might need to serialize not only the instance itself, but also the instance's related objects. In this case you can use the `to_dict()` function from the pony.orm.serialization module.

  * [`to_dict()`](http://doc.ponyorm.com/working_with_entity_instances.html#to_dict) - receives an entity instance or a list of instances and returns a dictionary structure which keeps the passed object(s) and immediate related objects

  * [`to_json()`](http://doc.ponyorm.com/working_with_entity_instances.html#to_json) – uses `to_dict()` and returns JSON representation of the `to_dict()` result

* [`Query.prefetch()`](http://doc.ponyorm.com/queries.html#Query.prefetch) – allows to specify which related objects or attributes should be loaded from the database along with the query result . Example:

```python
      select(s for s in Student).prefetch(Group, Department, Student.courses)
```

* [`obj.flush()`](http://doc.ponyorm.com/working_with_entity_instances.html#Entity.flush) – allows flush a specific entity to the database

* [`obj.get_pk()`](http://doc.ponyorm.com/working_with_entity_instances.html#Entity.get_pk) – return the primary key value for an entity instance

* [`py_check`](http://doc.ponyorm.com/entities.html#py_check) parameter for attributes added. This parameter allows you to specify a function which will be used for checking the value before it is assigned to the attribute. The function should return `True`/`False` or can raise `ValueError` exception if the check failed. Example:

```python
    class Student(db.Entity):
        name = Required(unicode)
        gpa = Required(float, py_check=lambda val: val >= 0 and val <= 5)
```

## New types:

* `time` and `timedelta` – now you can use these types for attribute declaration. Also you can use `timedelta` and a combination of `datetime` + `timedelta` types inside queries.

## New hooks:

* `after_insert`, `after_update`, `after_delete` - these hooks are called when an object was inserted, updated or deleted in the database respectively ([link](http://doc.ponyorm.com/working_with_entity_instances.html#entity-hooks))

## New features:

* Added support for pymysql – pure Python MySQL client. Currently it is used as a fallback for MySQLdb interface

## Other changes and bug fixes

* `obj.order_by()` method is deprecated, use `Entity.select().order_by()` instead
* `obj.describe()` now displays composite primary keys
* Fixes #50: PonyORM does not escape _ and % in LIKE queries
* Fixes #51: Handling of one-to-one relations in declarative queries
* Fixes #52: An attribute without a column should not have rbits & wbits
* Fixes #53: Column generated at the wrong side of "one-to-one" relationship
* Fixes #55: `obj.to_dict()` should do flush at first if the session cache is modified
* Fixes #57: Error in `to_dict()` when to-one attribute value is None
* Fixes #70: EntitySet allows to add and remove None
* Check that the entity name starts with a capital letter and throw exception if it is not then raise the `ERDiagramError: Entity class name should start with a capital letter` exception


# Pony ORM Release 0.5.3 (2014-08-12)

This release fixes the setup.py problem that was found after the previous release was uploaded to PyPI.


# Pony ORM Release 0.5.2 (2014-08-11)

This release is a step forward to Python 3 support. While the external API wasn't changed, the internals were significantly refactored to provide forward compatibility with Python 3. 

## Changes/features:

* New to_dict() method can be used to convert entity instance to dictionary. This method can be useful when you need to serialize an object to JSON or other format ([link](http://blog.ponyorm.com/2014/08/11/pony-orm-release-0-5-2/))

## Bugfixes:

* Now select() function and filter() method of the query object can accept lambdas with closures
* Some minor bugs were fixed


# Pony ORM Release 0.5.1 (2014-07-11)

## Changes/features:

Before this release, if a text attribute was defined without the max length specified (e.g. `name = Required(unicode)`), Pony set the maximum length equal to 200 and used SQL type `VARCHAR(200)`. Actually, PostgreSQL and SQLite do not require specifying the maximum length for strings. Starting with this release such text attributes are declared as `TEXT` in SQLite and PostgreSQL. In these DBMSes, the `TEXT` datatype has the same performance as `VARCHAR(N)` and doesn't have arbitrary length restrictions.

For other DBMSes default varchar limit was increased up to 255 in MySQL and to 1000 in Oracle.

## Bugfixes:

* Correct parsing of datetime values with T separator between date and time
* Entity.delete() bug fixed
* Lazy attribute loading bug fixed


# Pony ORM Release 0.5 (2014-05-31)

* New transaction model ([link](http://blog.ponyorm.com/2014/02/14/pony-orm-release-0-5-beta/))
* New method `Query.filter()` allows step-by-step query construction ([link](http://doc.ponyorm.com/queries.html?highlight=filter#Query.filter))
* New method `Database.bind()` simplifies testing and allows using different settings for development and production ([link](http://doc.ponyorm.com/database.html#binding-the-database-object-to-a-specific-database))
* New method `Query.page()` simplifies pagination ([link](http://doc.ponyorm.com/queries.html?highlight=filter#Query.page))
* New method `MyEntity.select_random(N)` is effective for large tables ([link](http://doc.ponyorm.com/queries.html#Entity.select_random))
* New method `Query.random(N)` for selecting random instances ([link](http://doc.ponyorm.com/queries.html#Query.random))
* Support of new `concat()` function inside declarative queries
* New `before_insert()`, `before_update()`, `before_delete()` entity instance hooks which can be overridden
* Ability to specify `sequence_name='seq_name'` for PrimaryKey attributes for Oracle database
* Ability to create new entity instances specifying the value of the primary key instead of the object
* Ability to read entity object attributes outside of the db_session
* Ability to use lambdas as a reference to an entity in relationship attribute declarations ([link](http://doc.ponyorm.com/entities.html?highlight=lambda#relationships))
* The names of tables, indexes and constraints in the database creation script now are sorted in the alphabetical order
* In MySQL and PostgreSQL Pony converts the table names to the lower case. In Oracle – to the upper case. In SQLite leaves as is.
* The option `options.MAX_FETCH_COUNT` is set to `None` by default now
* The support of PyGreSQL is discontinued, using psycopg2 instead
* Added `pony.__version__` attribute
* Multiple bugs were fixed
* Stability and performance improvements


# Pony ORM Release 0.4.9 (2013-10-25)

* Database `create_tables()`/`drop_all_tables()` methods
* `Database.drop_table(name)`, `Entity.drop_table()`, `Set.drop_table()` methods
* `Database.disconnect()` methods (allows SQLite files deletion after disconnection)
* Pony now automatically enables foreign key checks in SQLite
* `Entity.exists(...)` method added
* `distinct()` function added: `select((s.group, sum(distinct(s.gpa))) for s in Student)`
* Refactoring & bugfixes


# Pony ORM Release 0.4.8 (2013-08-27)

* Use standard transaction mode by default instead of optimistic mode
* `SELECT ... FOR UPDATE` support added: `select(...).for_update()[:]`
* `UUID` datatype support added
* Automatic foreign key indexes generation
* MySQL foreign key bug workaround added
* Check_tables parameter of `generate_mapping()` is deprecated
* Bug fixes


# Pony ORM Release 0.4.7 (2013-06-19)

* `@db_session` decorator is required for any database interaction;
* support of pickling/unpickling (queries and objects can be stored in memcached);
* lazy collections - don't load all the items if only one is needed;
* datetime precision now can be specified;
* multiple bugs were fixed.


# Pony ORM Release 0.4.6 (2013-05-17)
