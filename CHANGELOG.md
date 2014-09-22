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
