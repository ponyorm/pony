Getting Started with Pony
=============================

To install Pony, type the following command into the command prompt:

.. code-block:: text

    pip install pony

Pony may be installed on Python 2 beginning with version 2.6, and has no external dependencies.

To make sure Pony has been successfully installed, launch a Python interpreter in interactive mode and type::

    >>> from pony.orm import *

This imports the entire (and not very large) set of classes and functions necessary for working with Pony. Eventually you can choose what to import, but we recommend using ``import *`` at first.

The best way to become familiar with Pony is to play around with it in interactive mode. Let's create a sample database containing the entity class ``Person``, add three objects to it, and write a query. 


Creating a database
-----------------------------

Entities in Pony are connected to the database; this is why we first need to create the database object. In the Python interpreter, type::

    >>> db = Database('sqlite', ':memory:')

This command creates the connection object for the database. Its first parameter specifies the DBMS we want to work with. Currently Pony supports 4 types of databases: ``'sqlite'``, ``'mysql'``, ``'postgresql'`` and ``'oracle'``. The subsequent parameters are specific to each DBMS; they are the same ones you would use if you were connecting to the database through the DB-API module. For sqlite, either the filename of the database or the string ':memory:' must be indicated as a parameter, depending on where the database is being created. If the database is created in-memory, it will be deleted once the interactive session in Python is closed. In order to work with a database stored in a file, you can replace the previous line with the following::

    >>> db = Database('sqlite', 'test_db.sqlite', create_db=True)

In this case, if the database file does not exist, it will be created. In our example, we can use a database created in-memory. 


Defining entities
-------------------------------

Now, let's create two entities -- Person and Car. The entity Person has two attributes -- name and age, and Car has attributes make and model. The two entities have a one-to-many relationship. In the Python interpreter, type the following code:

.. code-block:: python

    >>> class Person(db.Entity):
    ...     name = Required(str)
    ...     age = Required(int)
    ...     cars = Set("Car")
    ... 
    >>> class Car(db.Entity):
    ...     make = Required(str)
    ...     model = Required(str)
    ...     owner = Required(Person)
    ... 
    >>> 

The classes that we have created are derived from ``db.Entity``. It means that they are not ordinary classes, but entities whose instances are stored in the database that the ``db`` variable points to. Pony allows you to work with several databases at the same time, but each entity belongs to one specific database. 

Inside the entity ``Person`` we have created three attributes -- ``name``, ``age`` and ``cars``. ``name`` and ``age`` are mandatory attributes; in other words, they can't have the value ``None``. ``name`` is an alphanumeric attribute, while ``age`` is numeric. 

The ``cars`` attribute has the type ``Car`` which means that this is a relationship. It can store a collection of instances of ``Car`` entity. ``"Car"`` is specified as a string here because we didn't declare the entity ``Car`` by that moment yet.

The entity ``Car`` has three mandatory attributes. ``make`` and ``model`` are strings and ``owner`` is the other side of the one-to-many relationship. Relationships in Pony are always defined by two attributes which represent both sides of a relationship. 

If we need to create a many-to-many relationship between two entities, then we should declare two ``Set`` attributes at both ends. Pony creates the intermediate table automatically.

The ``str`` type is used for representing an unicode string in Python 3. Python 2 has two types for representing strings - ``str`` and ``unicode``. Starting with the Pony Release 0.6, you can use either ``str`` or ``unicode`` for string attributes, both of them mean an unicode string. We recommend to use ``str`` for string attributes, because it looks more natural in Python 3.

If you need to see an entity definition in the interactive mode, you can use the ``show()`` function. Pass an entity class to this function in order to see the entity description:

.. code-block:: python

    >>> show(Person)
    class Person(Entity):
        id = PrimaryKey(int, auto=True)
        name = Required(str)
        age = Required(int)
        cars = Set(Car)

You may notice that the entity got one extra attribute named ``id``. Why did that happen?

Each entity must contain a primary key, which allows you to distinguish one entity from another. Since we have not set the primary key manually, it was created automatically. If the primary key is created automatically, it is named ``id`` and has a numeric format. If the key is created manually, it can be named in any way and can be either numeric or text. Pony also supports compound primary keys.

When the primary key is created automatically, it always has the option ``auto`` set to ``True``. It means that the value for this attribute will be assigned automatically using the database’s incremental counter or sequence.

Mapping entities to database tables
-----------------------------------------------

Now we need to create tables to store the objects' data. For this purpose, we need to call the following method on the ``Database`` object::

    >>> db.generate_mapping(create_tables=True)

The parameter ``create_tables=True`` indicates that, if the tables do not already exist, then they will be created using the ``CREATE TABLE`` command.

All entities connected to the database must be specified before calling ``generate_mapping()`` method.

Late database binding
-----------------------------------------------

Starting with Pony release 0.5 there is an alternative way of specifying the database parameters. Now you can create a database object first and then, after you declare entities, bind it to a specific database::

    ### module my_project.my_entities.py
    from pony.orm import *

    db = Database()
    class Person(db.Entity):
        name = Required(str)
        age = Required(int)
        cars = Set("Car")

    class Car(db.Entity):
        make = Required(str)
        model = Required(str)
        owner = Required(Person)

    ### module my_project.my_settings.py
    from my_project.my_entities import db

    db.bind('sqlite', 'test_db.sqlite', create_db=True)
    db.generate_mapping(create_tables=True)

This way you can separate entity definition from mapping it to a particular database. It can be useful for testing.


Using debug mode
-----------------------------------------------

Pony allows you to see on the screen (or in a log file, if configured) the SQL commands that it sends to the database. In order to turn on this mode, type::

    >>> sql_debug(True)

If this command is executed before calling ``generate_mapping()``, then during the creation of the tables, you will see the SQL code used to generate the tables.

Be default Pony sends debug information to stdout. If you have the `standard Python logging <https://docs.python.org/2/howto/logging.html>`_ configured, Pony will use it instead of stdout. Using Python logging you can store debug information in a file::

    import logging
    logging.basicConfig(filename='pony.log', level=logging.INFO)

Note, that we had to specify the ``level=logging.INFO`` because the default standard logging level is WARNING and Pony uses the INFO level for its messages by default. Pony uses two loggers: ``pony.orm.sql`` for SQL statements that it sends to the database and ``pony.orm`` for all other messages.


Creating entity instances and populating the database
------------------------------------------------------------------------------

Now, let's create five objects that describe three persons and two cars, and save this information in the database. To do this, we execute the following commands::

    >>> p1 = Person(name='John', age=20)
    >>> p2 = Person(name='Mary', age=22)
    >>> p3 = Person(name='Bob', age=30)
    >>> c1 = Car(make='Toyota', model='Prius', owner=p2)
    >>> c2 = Car(make='Ford', model='Explorer', owner=p3)
    >>> commit()

Pony does not save objects in the database as soon as they are created, instead they are saved only after the ``commit()`` command is executed. If the debug mode is turned on before executing ``commit()``, then you will see the five ``INSERT`` commands used to store the objects in the database.


Writing queries
--------------------------------------------

Now that we have a database with five objects saved in it, we can try some queries. For example, this is the query which returns a list of persons who are older than twenty years old::

    >>> select(p for p in Person if p.age > 20)
    <pony.orm.core.Query at 0x105e74d10>

The ``select`` function translates the Python generator into a SQL query and returns an instance of the ``Query`` class. This SQL query will be sent to the database once we start iterating over the query. One of the ways to get the list of objects is to apply the slice operator ``[:]`` to it::

    >>> select(p for p in Person if p.age > 20)[:]

    SELECT "p"."id", "p"."name", "p"."age"
    FROM "Person" "p"
    WHERE "p"."age" > 20

    [Person[2], Person[3]]

As the result you will see the text of the SQL query which was sent to the database and the list of extracted objects. When we print out the query result, an entity instance is represented by the entity name and its primary key written in square brackets: ``Person[2]``.

To order the resulting list we can use the ``order_by`` method of the query. And if we need only a portion of the result set, we can achieve this by using the slice operator as we would on a Python list. For example, if we want to sort all people by name and extract the first two objects, we can write::

    >>> select(p for p in Person).order_by(Person.name)[:2]

    SELECT "p"."id", "p"."name", "p"."age"
    FROM "Person" "p"
    ORDER BY "p"."name"
    LIMIT 2

    [Person[3], Person[1]]

Sometimes, when working in interactive mode, we want to see the values of all object attributes represented as a table. In order to do this, we can use the ``.show()`` method of the query result list::

    >>> select(p for p in Person).order_by(Person.name)[:2].show()

    SELECT "p"."id", "p"."name", "p"."age"
    FROM "Person" "p"
    ORDER BY "p"."name"
    LIMIT 2

    id|name|age
    --+----+---
    3 |Bob |30 
    1 |John|20

The ``.show()`` method doesn't display "to-many" attributes because it would require additional query to the database and could be bulky. That is why you can see no information about the related cars above. But if an instance has a "to-one" relationship, then it will be displayed::

    >>> Car.select().show()
    id|make  |model   |owner    
    --+------+--------+---------
    1 |Toyota|Prius   |Person[2]
    2 |Ford  |Explorer|Person[3]

If we don't want to get a list of objects, but need to iterate over the resulting sequence, we can use the ``for`` loop without using the slice operator::

    >>> persons = select(p for p in Person if 'o' in p.name)
    >>> for p in persons:
    ...     print p.name, p.age
    ...
    SELECT "p"."id", "p"."name", "p"."age"
    FROM "Person" "p"
    WHERE "p"."name" LIKE '%o%'

    John 20
    Bob 30

In the example above we get all Person objects where the name attribute contains the letter 'o' and display their name and age.

A query does not necessarily have to return entity objects only. For example, we can get a list of object attributes::

    >>> select(p.name for p in Person if p.age != 30)[:]

    SELECT DISTINCT "p"."name"
    FROM "Person" "p"
    WHERE "p"."age" <> 30

    [u'John', u'Mary']

Or a tuple::

    >>> select((p, count(p.cars)) for p in Person)[:]

    SELECT "p"."id", COUNT(DISTINCT "car-1"."id")
    FROM "Person" "p"
      LEFT JOIN "Car" "car-1"
        ON "p"."id" = "car-1"."owner"
    GROUP BY "p"."id"

    [(Person[1], 0), (Person[2], 1), (Person[3], 1)]

In the example above we get a list of tuples consisting of a person and the number of cars they own.

You can also run aggregate queries. Here is an example of a query which returns the maximum person's age::

    >>> print max(p.age for p in Person)
    SELECT MAX("p"."age")
    FROM "Person" "p"

    30

Pony allows you to write queries that are much more complex than the ones we have examined so far. You can read more on this in later sections of this manual.



Getting objects
--------------------------------------------------------------

To get an object by its primary key you specify the primary key value in square brackets::

    >>> p1 = Person[1]
    >>> print p1.name
    John

You may notice that no query was sent to the database. That happened because this object is already present in the database session cache. Caching reduces the number of requests that need to be sent to the database.

Getting objects by other attributes::

    >>> mary = Person.get(name='Mary')

    SELECT "id", "name", "age"
    FROM "Person"
    WHERE "name" = ?
    [u'Mary']

    >>> print mary.age
    22

In this case, even though the object had already been loaded to the cache, the query still had to be sent to the database because ``name`` is not a unique key. The database session cache will only be used if we lookup an object by its primary or unique key.

You can pass an entity instance to the function ``show()`` in order to display the entity class and attribute values::

    >>> show(mary)
    instance of Person
    id|name|age
    --+----+---
    2 |Mary|22



Updating an object 
-----------------------------------
::

    >>> mary.age += 1
    >>> commit()

Pony keeps track of changed attributes. When the operation ``commit()`` is executed, all objects that were updated during the current transaction will be saved in the database. Pony saves only changed attributes.


db_session
------------------------------------------

When you work with Python’s interactive shell you don't need to worry about the database session because it is maintained by Pony automatically. But when you use Pony in your application, all database interactions should be done within a database session. In order to do that you need to wrap the functions that work with the database with the ``@db_session`` decorator::

    @db_session
    def print_person_name(person_id):
        p = Person[person_id]
        print p.name
        # database session cache will be cleared automatically
        # database connection will be returned to the pool

    @db_session
    def add_car(person_id, make, model):
        Car(make=make, model=model, owner=Person[person_id])
        # commit() will be done automatically
        # database session cache will be cleared automatically
        # database connection will be returned to the pool

The ``@db_session`` decorator performs several very important actions upon function exit:

* Performs rollback of transaction if the function raises an exception
* Commits transaction if data was changed and no exceptions occurred 
* Returns the database connection to the connection pool
* Clears the database session cache

Even if a function just reads data and does not make any changes, it should use the ``db_session`` in order to return the connection to the connection pool.

The entity instances are valid only within the ``db_session``. If you need to render an HTML template using those objects, you should do this within the db_session.

Another option for working with the database is using ``db_session`` as the context manager instead of the decorator::

    with db_session:
        p = Person(name='Kate', age=33)
        Car(make='Audi', model='R8', owner=p)
        # commit() will be done automatically
        # database session cache will be cleared automatically
        # database connection will be returned to the pool



Writing SQL manually
------------------------------------------

If you need to write an SQL query manually, you can do it this way::

    >>> x = 25
    >>> Person.select_by_sql('SELECT * FROM Person p WHERE p.age < $x')

    SELECT * FROM Person p WHERE p.age < ?
    [25]

    [Person[1], Person[2]]

If you want to work with the database directly, avoiding entities altogether, you can use the ``select()`` method on the ``Database`` object::

    >>> x = 20
    >>> db.select('name FROM Person WHERE age > $x')
    SELECT name FROM Person WHERE age > ?
    [20]

    [u'Mary', u'Bob']


Pony examples
----------------------------------------------------------------------------

Instead of creating models manually, it may be easier to get familiar with Pony by importing some ready-made examples -- for instance, a simplified model of an online store. You can view the diagram for this example on the Pony website at this address: https://editor.ponyorm.com/user/pony/eStore

To import the example::

    >>> from pony.orm.examples.estore import *


At the initial launch, an SQLite database will be created with all the necessary tables. In order to populate it with the sample data, you can execute the following function, as indicated in the example file::

    >>> populate_database()

This function will create objects and place them in the database.

After the objects have been created, you can write a query. For example, you can find the country with the most customers::

    >>> select((customer.country, count(customer))
    ...        for customer in Customer).order_by(-2).first()

    SELECT "customer"."country", COUNT(DISTINCT "customer"."id")
    FROM "Customer" "customer"
    GROUP BY "customer"."country"
    ORDER BY 2 DESC
    LIMIT 1

In this case, we are grouping objects by country, sorting them by the second column (quantity of customers) in reverse order, and then extracting the country with the highest number of customers.

You can find more query examples in the ``test_queries()`` function from the ``pony.orm.examples.estore`` module.
