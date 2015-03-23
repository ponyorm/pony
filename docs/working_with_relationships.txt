.. _working_with_relationships_ref:

Working with relationships
======================================

In Pony, an entity can relate to other entities through relationships. Each relationship always has two ends, and defined by two entity attributes::

    class Person(db.Entity):
        cars = Set('Car')

    class Car(db.Entity):
        owner = Optional(Person)

In the example above we've defined one-to-many relationship between ``Person`` and ``Car`` entities using the ``cars`` and ``owner`` attributes. Let's add a couple more data attributes to our entities and then try some examples::

    from pony.orm import *

    db = Database('sqlite', ':memory:')

    class Person(db.Entity):
        name = Required(str)
        cars = Set('Car')

    class Car(db.Entity):
        make = Required(str)
        model = Required(str)
        owner = Optional(Person)

    db.generate_mapping(create_tables=True)

Now let's create instances of ``Person`` and ``Car`` entities::

    >>> p1 = Person(name='John')
    >>> c1 = Car(make='Toyota', model='Camry')
    >>> commit()

Normally, in your program, you don't need to call the function ``commit`` manually, because it should be done automatically by :ref:`db_session <db_session_ref>`. But when you work in interactive mode, you never leave a ``db_session``, that is why we need to commit manually if we want to store data in the database.


Establishing a relationship
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Right after we've created the instances ``p1`` and ``c1``, they don't have an established relationship. Let's check the values of the relationship attributes::

    >>> print c1.owner
    None

    >>> print p1.cars
    CarSet([])

The attribute ``cars`` has an empty set.

Now let's establish a relationship between these two instances::

    >>> c1.owner = p1

If we print the values of relationship attributes now, then we'll see the following::

    >>> print c1.owner
    Person[1]

    >>> print p1.cars
    CarSet([Car[1]])

When we assigned an owner to the ``Car`` instance, the ``Person``'s relationship attribute ``cars`` reflected the change immediately.

We also could establish a relationship by assigning the relationship attribute during the creation of the ``Car`` instance::

    >>> p1 = Person(name='John')
    >>> c1 = Car(make='Toyota', model='Camry', owner=p1)

In our example the attribute ``owner`` is optional, so we can assign a value to it at any time, either during the creation of the ``Car`` instance, or later.


Operations with collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The attribute ``cars`` of ``Person`` entity is represented as a collection and hence we can use regular operations that applicable to collections: add, remove, in, len, clear.

You can add or remove relationships using the ``add()`` and ``remove()`` methods::

    >>> p1.cars.remove(Car[1])
    >>> print p1.cars
    CarSet([])

    >>> p1.cars.add(Car[1])
    >>> print p1.cars
    CarSet([Car[1]])

You can check if a collection contains an element::

    >>> Car[1] in p1.cars
    True

Or make sure that there is no such element in the collection::

    >>> Car[1] not in p1.cars
    False

Check the collection length::

    >>> len(p1.cars)
    1

If you need to create an instance of a car and assign it with a particular person instance, there are several ways to do it. One of the options is to call the ``create()`` method of a collection attribute::

    >>> p1.cars.create(model='Toyota', make='Prius')
    >>> commit()

Now we can check that a new ``Car`` instance was added to the ``cars`` collection attribute of the ``Person`` instance::

    >>> print p1.cars
    CarSet([Car[2], Car[1]])
    >>> p1.cars.count()
    2

You can iterate over a collection attribute::

    >>> for car in p1.cars:
    ...     print car.model

    Toyota
    Camry


Attribute lifting
-----------------------

In Pony, the collection attributes provide an attribute lifting capability: a collection gets its items' attributes.

    >>> show(Car)
    class Car(Entity):
        id = PrimaryKey(int, auto=True)
        make = Required(str)
        model = Required(str)
        owner = Optional(Person)
    >>> p1 = Person[1]
    >>> print p1.cars.model
    Multiset({u'Camry': 1, u'Prius': 1})

Here we print out the entity class attributes using the ``show()`` function and then print the value of the ``model`` attribute of the ``cars`` relationship attribute. The ``cars`` attribute has all the attributes of the ``Car`` entity: ``id``, ``make``, ``model`` and ``owner``. In Pony we call this a Multiset and it is implemented using a dictionary. The dictionary's key represents the value of the attribute - 'Camry' and 'Prius' in our example. And the dictionary's value shows how many times it encounters in this collection.

.. code-block:: python

    >>> print p1.cars.make
    Multiset({u'Toyota': 2})

``Person[1]`` has two Toyotas.

We can iterate over the multiset::

    >>> for m in p1.cars.make:
    ...     print m
    ...
    Toyota
    Toyota


Multisets
---------------------------------

TBD:

* aggregate functions
* distinct
* subquery in declarative queries



Collection attribute parameters
-----------------------------------------

Collection attributes are used for defining a 'to-many' side of a relationship. They can be used for defining one-to-many or many-to-many relationships. For example::

    class Photo(db.Entity):
        tags = Set('Tag', lazy=True, table='Photo_to_Tag')

    class Tag(db.Entity):
        photos = Set(Photo)

Here the attributes ``tags`` and ``photos`` are collections.

Below are the parameters which you can specify while creating a collection attribute.


.. class:: Set

   Represents the to-many relationship.

   .. py:attribute:: lazy

      When we access a specific collection item (check if an element belongs to a collection, add or delete items), Pony loads the whole collection to the ``db_session`` cache. Usually it increases the performance by reducing the database round trips. But if you have large collections you may prefer not to load them into the cache. Setting ``lazy=True`` tells Pony that it shouldn't load the collection to the cache, but always send queries to the database. Default is ``lazy=False``.

   .. py:attribute:: reverse

      Specifies the name of the attribute of related entity which is used for the relationship. This parameter should be used when there are more than one relationship between two entities.

   .. py:attribute:: table

      This parameter is used for many-to-many relationships only and allows you to specify the name of the intermediate table used for representing this relationship in the database.

   .. py:attribute:: column
                     columns
                     reverse_column
                     reverse_columns

      These parameters are used for many-to-many relationships and allows you to specify the name of the intermediate columns. The ``columns`` and ``reverse_columns`` parameters receive a list and used when the entity has a composite key. Typically you use the ``column`` or ``columns`` parameters in both relationship attributes if you don't like the default column name.

   .. py:attribute:: cascade_delete

      Boolean value which controls the cascade deletion of the related objects. Default value depends on the another side of the relationship. If it is ``Optional`` - the default value is ``False`` and if it is ``Required`` then ``True``.

   .. py:attribute:: nplus1_threshold

      This parameter is used for fine tuning the threshold used for the N+1 problem solution.


Collection instance methods
------------------------------------

You can treat a collection attribute as a regular Python collection and use standard operations like ``in``, ``not in``, ``len``:

.. class:: Set

    .. _set_len_ref:

    .. py:method:: len()

       Returns the number of objects in the collection. If the collection is not loaded into cache, this methods loads all the collection instances into the cache first, and then returns the number of objects. Use this method if you are going to iterate over the objects and you need them loaded into the cache. If you don't need the collection to be loaded into the memory, you can use the :ref:`count() <set_count_ref>` method.

    .. code-block:: python

        >>> p1 = Person[1]
        >>> Car[1] in p1.cars
        True
        >>> len(p1.cars)
        2

Also there is a number of methods which you can call on a collection attribute.

.. class:: Set

   Below you can find methods which you can call on a 'to-many' relationship attribute.

   .. py:method:: add(item)
                  add(iter)

      Adds instances to a collection and establishes a two-way relationship between entity instances::

          photo = Photo[123]
          photo.tags.add(Tag['Outdoors'])

      Now the instance of the ``Photo`` entity with the primary key 123 has a relationship with the ``Tag['Outdoors']`` instance. The attribute ``photos`` of the ``Tag['Outdoors']`` instance contains the reference to the ``Photo[123]`` as well.

      We can also establish several relationships at once passing the list of tags to the ``add()`` method::

          photo.tags.add([Tag['Party'], Tag['New Year']])

   .. py:method:: remove(item)
                  remove(iter)

      Removes an item or items from the collection and thus breaks the relationship between entity instances.

   .. py:method:: clear()

      Removes all items from the collection which means breaking relationships between entity instances.

   .. py:method:: is_empty()

      Returns ``False`` if there is at lease one relationship and ``True`` if this attribute has no relationships.

   .. py:method:: copy()

      Returns a Python ``set`` object which contains the same items as the given collection.

   .. _set_count_ref:

   .. py:method:: count()

      Returns the number of objects in the collection. This method doesn't load the collection instances into the cache, but generates an SQL query which returns the number of objects from the database. If you are going to work with the collection objects (iterate over the collection or change the object attributes), you might want to use the :ref:`len() <set_len_ref>` method.

   .. py:method:: create(**kwargs)

      Creates an returns an instance of the related entity and establishes a relationship with it::

          new_tag = Photo[123].tags.create(name='New tag')

      is an equivalent of the following::

          new_tag = Tag(name='New tag')
          Photo[123].tags.add(new_tag)

   .. py:method:: load()

      Loads all related objects from the database.


Collection class methods
------------------------------------

This method can be called on the entity class, not instance. For example::

    from pony.orm import *

    db = Database('sqlite', ':memory:')

    class Photo(db.Entity):
        tags = Set('Tag')

    class Tag(db.Entity):
        photos = Set(Photo)

    db.generate_mapping(create_tables=True)

    Photo.tags.drop_table() # drops the Photo-Tag intermediate table


.. class:: Set

   .. py:method:: drop_table(with_all_data=False)

      Drops the intermediate table which is created for establishing many-to-many relationship. If the table is not empty and ``with_all_data=False``, the method raises the ``TableIsNotEmpty`` exception and doesn't delete anything. Setting the ``with_all_data=True`` allows you to delete the table even if it is not empty.


.. _col_queries_ref:

Collection queries
----------------------------

Starting with the release 0.6.1, Pony introduces queries for the relationship attributes.

You can apply :py:func:`select`, :py:meth:`Query.filter`, :py:meth:`Query.order_by`, :py:meth:`Query.page`, :py:meth:`Query.limit`, :py:meth:`Query.random` methods to the relationships to-many. The method names ``select`` and ``filter`` are synonyms.

Below you can find several examples of using these methods. We'll use the University schema for showing these queries, here are `python entity definitions <https://github.com/ponyorm/pony/blob/orm/pony/orm/examples/university1.py>`_ and  `Entity-Relationship diagram <https://editor.ponyorm.com/user/pony/University>`_.

The example below selects all students with the ``gpa`` greater than 3 within the group 101::

    g = Group[101]
    g.students.filter(lambda student: student.gpa > 3)[:]

This query can be used for displaying the second page of group 101 student's list ordered by the ``name`` attribute::

    g.students.order_by(Student.name).page(2, pagesize=3)

The same query can be also written in the following form::

    g.students.order_by(lambda s: s.name).limit(3, offset=3)

The following query returns two random students from the group 101::

    g.students.random(2)

And one more example. This query returns the first page of courses which were taken by ``Student[1]`` in the second semester, ordered by the course name::

    s = Student[1]
    s.courses.select(lambda c: c.semester == 2).order_by(Course.name).page(1)

