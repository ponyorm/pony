Relationships
================================

Entities can relate to each other. A relationship between two entities is defined by using two attributes which specify both ends of a relationship::

   class Customer(db.Entity):
       orders = Set("Order")

   class Order(db.Entity):
       customer = Required(Customer)

In the example above we have two relationship attributes: ``orders`` and ``customer``. When we define the entity ``Customer``, the entity ``Order`` is not defined yet. That is why we have to put quotes around ``Order``. Another option is to use lambda::

   class Customer(db.Entity):
       orders = Set(lambda: Order)

This can be useful if you want your IDE to check the names of declared entities and highlight typos.

Some mappers (e.g. Django) require defining relationships on one side only. Pony requires defining relationships on both sides explicitly (as The Zen of Python reads: Explicit is better than implicit), which allows the user to see all relationships from the perspective of each entity.

All relationships are bidirectional. If you update one side of a relationship, the other side will be updated automatically. For example, if we create an instance of ``Order`` entity, the customer’s set of orders will be updated to include this new order.

There are three types of relationships: one-to-one, one-to-many and many-to-many. A one-to-one relationship is rarely used, most relations between entities are one-to-many and many-to-many. If two entities have one-to-one relationship it often means that they can be combined into a single entity. If your data diagram has a lot of one-to-one relationships, then it may signal that you need to reconsider entity definitions.

One-to-many relationship
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is an example of one-to-many relationship::

    class Order(db.Entity):
        items = Set("OrderItem")

    class OrderItem(db.Entity):
        order = Required(Order)

In the example above the instance of ``OrderItem`` cannot exist without an order. If we want to allow an instance of ``OrderItem`` to exist without being assigned to an order, we can define the ``order`` attribute as ``Optional``::

    class Order(db.Entity):
        items = Set("OrderItem")

    class OrderItem(db.Entity):
        order = Optional(Order)

Many-to-many relationship
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to create many-to-many relationship you need to define both ends of the relationship as ``Set`` attributes::

    class Product(db.Entity):
        tags = Set("Tag")

    class Tag(db.Entity):
        products = Set(Product)

In order to implement this relationship in the database, Pony will create an intermediate table. This is a well known solution which allows you to have many-to-many relationships in relational databases.

One-to-one relationship
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to create a one-to-one relationship, the relationship attributes should be defined as ``Optional``-``Required`` or as ``Optional``-``Optional``::

    class Person(db.Entity):
        passport = Optional("Passport")

    class Passport(db.Entity):
        person = Required("Person")

Defining both attributes as ``Required`` is not allowed because it doesn’t make sense.

Self-references
~~~~~~~~~~~~~~~~~~~~~~

An entity can relate to itself using a self-reference relationship. Such relationships can be of two types: symmetric and non-symmetric. A non-symmetric relationship is defined by two attributes which belong to the same entity.

The specifics of the symmetrical relationship is that the entity has just one relationship attribute specified, and this attribute defines both sides of the relationship. Such relationship can be either one-to-one or many-to-many. Here are examples of self-reference relationships::

   class Person(db.Entity):
       name = Required(str)
       spouse = Optional("Person", reverse="spouse") # symmetric one-to-one
       friends = Set("Person", reverse="friends")    # symmetric many-to-many
       manager = Optional("Person", reverse="employees") # one side of non-symmetric
       employees = Set("Person", reverse="manager") # another side of non-symmetric


Multiple relationships between two entities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When two entities have more than one relationship between them, Pony requires the reverse attributes to be specified. This is needed in order to let Pony know which pair of attributes are related to each other. Let’s consider the data diagram where a user can write tweets and also can favorite them::

   class User(db.Entity):
       tweets = Set("Tweet", reverse="author")
       favorites = Set("Tweet", reverse="favorited")

   class Tweet(db.Entity):
       author = Required(User, reverse="tweets")
       favorited = Set(User, reverse="favorites")

In the example above we have to specify the option ``reverse``. If you will try to generate mapping for the entities definition without the ``reverse`` specified, you will get the exception ``pony.orm.core.ERDiagramError``: ``"Ambiguous reverse attribute for Tweet.author"``.
That happens because in this case the attribute ``author`` can technically relate either to the attribute ``tweets`` or to ``favorites`` and Pony has no information on which one to use.


