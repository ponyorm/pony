# Pony ORM Release 0.6.2 (2015-01-11)

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
