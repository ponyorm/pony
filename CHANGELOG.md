# Pony ORM Release 0.5.1 (2014-07-11)

## Changes/features:

Before this release, if a text attribute was defined without the max length specified (e.g. `name = Required(unicode)`), Pony set the maximum length equal to 200 and used SQL type `VARCHAR(200)`. Actually, PostgreSQL and SQLite do not require specifying the maximum length for strings. Starting with this release such text attributes are declared as `TEXT` in SQLite and PostgreSQL. In these DBMSes, the `TEXT` datatype has the same performance as `VARCHAR(N)` and doesn't have arbitrary length restrictions.

For other DBMSes default varchar limit was increased up to 255 in MySQL and to 1000 in Oracle.

## Bugfixes:

* Correct parsing of datetime values with T separator between date and time
* Entity.delete() bug fixed
* Lazy attribute loading bug fixed


# Pony ORM Release 0.5 (2014-05-31)

* New transaction model ([link](http://blog.ponyorm.com/?p=125))
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
* Ability to use lambdas as a reference to an entitys in relationship attribute declarations ([link](http://doc.ponyorm.com/entities.html?highlight=lambda#relationships))
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
