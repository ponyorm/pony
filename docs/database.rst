Database
==============

Before you can start working with entities you have to create a ``Database`` object. This object manages database connections using a connection pool. The ``Database`` object is thread safe and can be shared between all threads in your application. The ``Database`` object allows you to work with the database directly using SQL, but most of the time you will work with entities and let Pony generate SQL statements in order to make the corresponding changes in the database. Pony allows you to work with several databases at the same time, but each entity belongs to one specific database.

Mapping entities to the database can be divided into four steps:

* Creating a database object
* Defining entities which are related to the database object
* Binding the database object to a specific database
* Mapping entities to the database tables

Creating a database object
-----------------------------------------

At this step we simply create an instance of the ``Database`` class::

    db = Database()

Although you can pass the database connection parameters right here, often it is more convenient to do it at a later stage using the ``db.bind()`` method. This way you can use different databases for testing and production.

The ``Database`` instance has an attribute ``Entity`` which represents a base class to be used for entities declaration.


Defining entities which are related to the database object
---------------------------------------------------------------------------------------

Entities should inherit from the base class of the ``Database`` object::

    class MyEntity(db.Entity):
        attr1 = Required(str)

We'll talk about entities definition in detail in the next chapter. Now, let's see the next step in mapping entities to a database.

Binding the database object to a specific database
--------------------------------------------------------------

Before we can map entities to a database, we need to connect to the database. At this step we should use the ``bind()`` method::

    db.bind('postgres', user='', password='', host='', database='')

The first parameter of this method is the name of the database provider. The database provider is a module which resides in the ``pony.orm.dbproviders`` package and which knows how to work with a particular database. After the database provider name you should specify parameters which will be passed to the ``connect()`` method of the corresponding DBAPI driver.


Database providers
------------------------------

Currently Pony can work with four database systems: SQLite, PostgreSQL, MySQL and Oracle, with the corresponding Pony provider names: ``'sqlite'``, ``'postgres'``, ``'mysql'`` and ``'oracle'``.  Pony can easily be extended to incorporate additional database providers.

During the ``bind()`` call, Pony tries to establish a test connection to the database. If the specified parameters are not correct or the database is not available, an exception will be raised. After the connection to the database was established, Pony retrieves the version of the database and returns the connection to the connection pool.


SQLite
~~~~~~~~~~~~~~~~~~~~~~

Using SQLite database is the easiest way to work with Pony because there is no need to install a database system separately - the SQLite database system is included in the Python distribution. It is a perfect choice for beginners who want to experiment with Pony in the interactive shell. In order to bind the ``Database`` object a SQLite database you can do the following::
   
    db.bind('sqlite', 'filename', create_db=True)

Here 'filename' is the name of the file where SQLite will store the data. The filename can be absolute or relative.

.. note:: If you specify a relative path, that path is appended to the directory path of the Python file where this database was created (and not to the current working directory). We did it this way because sometimes a programmer doesn’t have the control over the current working directory (e.g. in mod_wsgi application). This approach allows the programmer to create applications which consist of independent modules, where each module can work with a separate database.

When working in the interactive shell, Pony requires that you to always specify the absolute path of the storage file.

If the parameter ``create_db`` is set to ``True`` then Pony will try to create the database if such filename doesn’t exists.  Default value for ``create_db`` is ``False``.

.. note:: Normally SQLite database is stored in a file on disk, but it also can be stored entirely in memory. This is a convenient way to create a SQLite database when playing with Pony in the interactive shell, but you should remember, that the entire in-memory database will be lost on program exit. Also you should not work with the same in-memory SQLite database simultaneously from several threads because in this case all threads share the same connection due to SQLite limitation.
 
  In order to bind with an in-memory database you should specify ``:memory:`` instead of the filename::

      db.bind('sqlite', ':memory:')

  There is no need in the parameter ``create_db`` when creating an in-memory database. 

.. note:: By default SQLite doesn’t check foreign key constraints. Pony always enables the foreign key support by sending the command ``PRAGMA foreign_keys = ON;`` starting with the release 0.4.9.



PostgreSQL
~~~~~~~~~~~~~~~~~~~~~~~

Pony uses psycopg2 driver in order to work with PostgreSQL. In order to bind the ``Database`` object to PostgreSQL use the following line::

    db.bind('postgres', user='', password='', host='', database='')

All the parameters that follow the Pony database provider name will be passed to the ``psycopg2.connect()`` method. Check the `psycopg2.connect documentation <http://initd.org/psycopg/docs/module.html#psycopg2.connect>`_ in order to learn what other parameters you can pass to this method.


MySQL
~~~~~~~~~~~~~~~
::

    db.bind('mysql', host='', user='', passwd='', db='')

Pony tries to use the MySQLdb driver for working with MySQL. If this module cannot be imported, Pony tries to use pymysql. See the `MySQLdb <http://mysql-python.sourceforge.net/MySQLdb.html#functions-and-attributes>`_ and `pymysql <https://pypi.python.org/pypi/PyMySQL>`_ documentation for more information regarding these drivers.

Oracle
~~~~~~~~~~~~~~~~
::

    db.bind('oracle', 'user/password@dsn')

Pony uses cx_Oracle driver for connecting to Oracle databases. More information about the parameters which you can use for creating a connection to Oracle database can be found `here <http://cx-oracle.sourceforge.net/html/module.html>`_. 


Mapping entities to the database tables
----------------------------------------------------------

After the ``Database`` object is created, entities are defined, and a database is bound, the next step is to map entities to the database tables::

    db.generate_mapping(check_tables=True, create_tables=False)

If the parameter ``create_tables`` is set to ``True`` then Pony will try to create tables if they don’t exist. The default value for ``create_tables`` is ``False`` because in most cases tables already exist. Pony generates the names of the database tables and columns automatically, but you can override this behavior if you want. See more details in the :ref:`Mapping customization <mapping_customization>` chapter. Also this parameter makes Pony to check if foreign keys and indexes exist and create them if they are missing.

After the ``create_tables`` option is processed, Pony does a simple check:  it sends SQL queries to the database which check that all entity tables and column names exist. At the same time this check doesn’t catch situations when the table has extra columns or when the type of a particular column doesn’t match. You can switch this check off by passing the parameter ``check_tables=False``. It can be useful when you want to generate mapping and create tables for your entities later, using the method ``db.create_tables()``.

The method ``db.create_tables()`` checks the existing mapping and creates tables for entities if they don’t exist. Also, Pony checks if foreign keys and indexes exist and create them if they are missing.


Early database binding
----------------------------------------------------------------------------

You can combine the steps 'Creating a database object' and 'Binding the database object to a specific database' into one step by passing the database parameters during the database object creation::

    db = Database('sqlite', 'filename', create_db=True)

    db = Database('postgres', user='', password='', host='', database='')

    db = Database('mysql', host='', user='', passwd='', db='')

    db = Database('oracle', 'user/password@dsn')

It is the same set of parameters which you can pass to the ``bind()`` method. If you pass the parameters during the creation of the ``Database`` object, then there is no need in calling the ``bind()`` method later - the database will be already bound to the instance.


Methods and attributes of the Database object
---------------------------------------------------------

.. class:: Database

   .. py:method:: bind(provider, *args, **kwargs)

      Binds entities to a database. The first parameter - ``provider`` is the name of the database provider. The database provider is a module which resides in the ``pony.orm.dbproviders`` package and which knows how to work with a particular database. After the database provider name you should specify parameters which will be passed to the ``connect()`` method of the corresponding DBAPI driver.

      Examples of ``bind()`` parameters for supported databases::

          db.bind('sqlite', 'filename', create_db=True)

          db.bind('postgres', user='', password='', host='', database='')

          db.bind('mysql', host='', user='', passwd='', db='')

          db.bind('oracle', 'user/password@dsn')

      During the ``bind()`` call, Pony tries to establish a test connection to the database. If the specified parameters are not correct or the database is not available, an exception will be raised. After the connection to the database was established, Pony retrieves the version of the database and returns the connection to the connection pool.

      The method can be called only once for a database object. All consequent calls of this method on the same database will throw the ``TypeError('Database object was already bound to ... provider')`` exception.

   .. py:method:: generate_mapping(check_tables=True, create_tables=False)

      Map declared entities to the corresponding tables in the database.
      ``create_tables=True`` - create tables, foreign key references and indexes if they don’t exist.
      ``check_tables=False`` - switch the table checks off. This check only verifies if the table name and  attribute names match. It doesn’t catch situations when the table has extra columns or when the type of a particular column doesn’t match.

   .. py:method:: create_tables()

      Check the existing mapping and create tables for entities if they don’t exist. Also, Pony checks if foreign keys and indexes exist and create them if they are missing.


   .. py:method:: drop_all_tables(with_all_data=False)

      Drop all tables which are related to the current mapping. When this method is called without parameters, Pony will drop tables only if none of them contain any data. In case at least one of them is not empty the method will raise the ``TableIsNotEmpty`` exception without dropping any table. In order to drop tables with the data you should pass the parameter ``with_all_data=True``.


   .. py:method:: drop_table(table_name, if_exists=False, with_all_data=False)

      Drops the ``table_name`` table. If such table doesn’t exist the method raises the exception ``TableDoesNotExist``. Note, that the table_name is case sensitive.

      You can pass the entity class as the ``table_name`` parameter. In this case Pony will try to delete the table associated with the entity.

      If the parameter ``if_exists`` is set to ``True``, then it will not raise the ``TableDoesNotExist`` exception if there is no such table in the database. If the table is not empty the method will raise the ``TableIsNotEmpty`` exception.

      In order to drop tables with the data you should pass the parameter ``with_all_data=True``.

      If you need to delete the table which is mapped to an entity, you should use the method :meth:`~Entity.drop_table` of an entity: it will use the right letter case for the entity table name.


Methods for working with transactions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. class:: Database

   .. py:method:: commit()

      Saves all changes which were made within the current ``db_session`` using the :py:meth:`flush()<Database.flush>` method and commits the transaction to the database.

      A programmer can call ``commit()`` more than once within the same ``db_session``. In this case the ``db_session`` cache keeps the cached objects after commits. This allows Pony to use the same objects in the transaction chain. The cache will be cleaned up when ``db_session`` is finished or the transaction is rolled back.

   .. py:method:: rollback()

      Rolls back the current transaction and clears the ``db_session`` cache.

   .. py:method:: flush()

      Saves the changes accumulated in the ``db_session`` cache to the database. You may never have a need to call this method manually. Pony always saves the changes accumulated in the cache automatically before executing the following methods: ``select()``, ``get()``, ``exists()``, ``execute()`` and ``commit()``.


Database object attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. class:: Database

   .. py:attribute:: Entity

      This attribute represents a base class which should be inherited by all entities which are mapped to the particular database::

          db = Database()

          class Person(db.Entity):
              name = Required(str)
              age = Required(int)

   .. py:attribute:: last_sql

      Read-only attribute which keeps the text of the last SQL statement. It can be used for debugging.


Methods for raw SQL access
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. class:: Database

   .. py:method:: select(sql)

      Executes the SQL statement in the database and returns a list of tuples. Pony gets a database connection from the connection pool and returns it after the query is completed. The word ``select`` can be omitted in the SQL statement - Pony will add the `select` keyword automatically when it is necessary::

          select("* from Person")

      We did it because the method name speaks for itself and this way the query looks more concise. If the query returns just one column, then, for the convenience, the result will be a list of values, not tuples:

      .. code-block:: python

          db.select("name from Person")

      The method above returns: ["John", "Mary", "Bob"]

      If a query returns more than one column and the names of table columns are valid Python identifiers, then you can access them as attributes:

      .. code-block:: python

          for row in db.select("name, age from Person"):
              print row.name, row.age

      Pony has a limit for the number of rows which can be returned by the ``select`` method. This limit is specified by the ``pony.options.MAX_FETCH_COUNT`` parameter (1000 by default). If ``select`` returns more than ``MAX_FETCH_COUNT`` rows Pony raises the ``TooManyRowsFound`` exception. You can change this value, although we don’t recommend doing this because if a query returns more than 1000 rows then it is likely that there is a problem with the application design. The results of ``select`` are stored in memory and if the number of rows is very large, applications can face scalability problems.

      Before executing ``select``, Pony flushes all the changes from the cache using the :ref:`flush() <flush_ref>` method.

   .. py:method:: get(sql)

      Use the ``get`` method when you need to select one row or just one value from the database::

          name = db.get("age from Person where id = $id")

      The word ``select`` can be omitted in the SQL statement - Pony will add the `select` keyword automatically if it is necessary. If the table Person has a row with the specified id, then the variable ``age`` will be assigned with the corresponding value of ``int`` type. The ``get`` method assumes that the query returns exactly one row. If the query returns nothing then Pony raises ``RowNotFound`` exception. If the query returns more than one row, the exception ``MultipleRowsFound`` will be raised.

      If you need to select more than one column you can do it this way::

           name, age = db.get("name, age from Person where id = $id")

      If your request returns a lot of columns then you can assign the resulting tuple of the ``get`` method to a variable and work with it the same way as it is described in ``select`` method.

      Before executing ``get``, Pony flushes all the changes from the cache using the :ref:`flush() <flush_ref>` method.

   .. py:method:: exists(sql)

      The ``exists`` method is used in order to check if the database has at least one row with the specified parameters. The result will be ``True`` or ``False``::

          if db.exists("* from Person where name = $name"):
              print "Person exists in the database"

      The word `select` can be omitted in the beginning of the SQL statement.

      Before executing this method, Pony flushes all the changes from the cache using the :ref:`flush() <flush_ref>` method.

   .. py:method:: insert(table_name, returning=None, **kwargs)
                  insert(entity, returning=None, **kwargs)

      Insert new rows into a table. This command bypasses the identity map cache and can be used in order to increase the performance when we need to create a lot of objects and not going to read them in the same transaction. Also you can use the ``db.execute()`` method for this purpose. If you need to work with those objects in the same transaction it is better to create instances of entities and have Pony to save them in the database on ``commit()``.

      ``table_name`` - is the name of the table into which the data will be inserted, the name is case sensitive. Instead of the ``table_name`` you can use the ``entity`` class. In this case Pony will insert into the table associated with the ``entity``.

      The ``returning`` parameter allows you to specify the name of the column that holds the automatically generated primary key. If you want the ``insert`` method to return the value which is generated by the database, you should specify the name of the primary key column::

          new_id = db.insert("Person", name="Ben", age=33, returning='id')

   .. py:method:: execute(sql)

      This method allows you to execute arbitrary (raw) SQL statements::

          cursor = db.execute("""create table Person (
                                     id integer primary key autoincrement,
                                     name text,
                                     age integer
                              )""")
          name, age = "Ben", 33
          cursor = db.execute("insert into Person (name, age) values ($name, $age)")

      All the parameters can be passed into the query using the Pony unified way, independently of the DBAPI provider, using the ``$`` sign. In the example above we pass ``name`` and ``age`` parameters into the query.

      It is possible to have a Python expressions inside the query text, for example::

          x = 10
          a = 20
          b = 30
          db.execute("SELECT * FROM Table1 WHERE column1 = $x and column2 = $(a + b)")

      If you need to use the $ sign as a string literal inside the query, you need to escape it using another $ (put two $ signs in succession: $$).

      The method returns the DBAPI cursor. Before executing the provided SQL, Pony flushes all the changes from the cache using the :ref:`flush() <flush_ref>` method.


   .. py:method:: get_connection()

      Get an active database connection. It can be useful if you want to work with the DBAPI interface directly. This is the same connection which is used by the ORM itself. The connection will be reset and returned to the connection pool on leaving the ``db_session`` context or when the database transaction rolls back. This connection can be used only within the ``db_session`` scope where the connection was obtained.

   .. py:method:: disconnect()

      Close the database connection for the current thread if it was opened.


Database statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. class:: Database

   The ``Database`` object keeps statistics on executed queries. You can check which queries were executed more often and how long it took to execute them as well as many other parameters. Pony keeps all statistics separately for each thread. If you want to see the aggregated statistics for all threads then you need to call the ``merge_local_stats()`` method.

   .. py:attribute:: local_stats

      This is a dictionary which keeps the SQL query statistics for the current thread. The key of this dictionary is the SQL statement and the value is an object of the ``QueryStat`` class.

   .. py:class:: QueryStat

      The class has a set of attributes which accumulate the corresponding values: ``avg_time``, ``cache_count``, ``db_count``, ``max_time``, ``merge``, ``min_time``, ``query_executed``, ``sql``, ``sum_time``

   .. py:method:: merge_local_stats()

      This method merges the statistics from the current thread into the global statistics. You can call this method at the end of the HTTP request processing.

   .. py:attribute:: global_stats

      This is a dictionary where the statistics for executed SQL queries is aggregated from all threads. The key of this dictionary is the SQL statement and the value is an object of the ``QueryStat`` class.



Using Database object for raw SQL queries
----------------------------------------------------

Typically you will work with entities and let Pony interact with the database, but Pony also allows you to work with the database using SQL, or even combine both ways. Of course you can work with the database directly using the DBAPI interface, but using the ``Database`` object gives you the following advantages:

* Automatic transaction management using the :ref:`db_session <db_session_ref>` decorator or context manager. All data will be stored to the database after the transaction is finished, or rolled back if an exception happened.
* Connection pool. There is no need to keep track of database connections. You have the connection when you need it and when you have finished your transaction the connection will be returned to the pool.
* Unified database exceptions. Each DBAPI module defines its own exceptions. Pony allows you to work with the same set of exceptions when working with any database. This helps you to create applications which can be ported from one database to another.
* Unified way of passing parameters to SQL queries with the protection from injection attacks. Different database drivers use different paramstyles - the DBAPI specification offers 5 different ways of passing parameters to SQL queries. Using the ``Database`` object you can use one way of passing parameters for all databases and eliminate the risk of SQL injection.
* Automatic unpacking of single column results when using ``get`` or ``select`` methods of the ``Database`` object. If the ``select`` method returns just one column, Pony returns a list of values, not a list of tuples each of which has just one item, as it does DBAPI. If the ``get`` method returns a single column it returns just value, not a tuple consisting of one item. It’s just convenient.
* When the methods ``select`` or ``get`` return more than one column, Pony uses smart tuples which allow accessing items as tuple attributes using column names, not just tuple indices.

In other words the ``Database`` object helps you save time completing routine tasks and provides convenience and uniformity.


Using parameters in raw SQL queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With Pony you can easily pass parameters into SQL queries. In order to specify a parameter you need to put the `$` sign before the variable name::

    x = "John"
    data = db.select("* from Person where name = $x")

When Pony encounters such a parameter within the SQL query it gets the variable value from the current frame (from globals and locals) or from the dictionary which is passed as the second parameter. In the example above Pony will try to get the value for ``$x`` from the variable ``x`` and will pass this value as a parameter to the SQL query which eliminates the risk of SQL injection. Below you can see how to pass a dictionary with the parameters::

    data = db.select("* from Person where name = $x", {"x" : "Susan"})

This method of passing parameters to the SQL queries is very flexible and allows using not only single variables, but any Python expression. In order to specify an expression you need to put it in parentheses after the $ sign::

    data = db.select("* from Person where name = $(x.lower()) and age > $(y + 2)")


