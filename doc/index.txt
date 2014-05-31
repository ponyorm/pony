What is Pony ORM?
======================

The acronym ORM stands for "object-relational mapper". An ORM allows developers to work with the contents of a database in the form of objects. A relational database contains rows that are stored in tables. However, when writing a program in a high level object-oriented language, it is considerably more convenient when the data retrieved from the database can be accessed in the form of objects. Pony ORM is a library for Python language that allows you to conveniently work with objects which are stored as rows in a relational database.

There are other popular mappers implemented in Python such as Django and SQLAlchemy, but we propose that Pony has certain distinct advantages:

 * An exceptionally convenient syntax for writing queries
 * Automatic query optimization
 * An elegant solution for the N+1 problem
 * A graphical database schema editor

In comparison to Django, Pony supports:

 * The IdentityMap pattern
 * Automatic transaction management 
 * Automatic caching of queries and objects
 * Full support of composite keys
 * The ability to easily write queries using LEFT JOIN, HAVING and other features of SQL

One interesting feature of Pony is that it allows you to interact with databases in pure Python in the form of generator expressions, which are then translated into SQL. Such queries may easily be written by a programmer familiar with Python, even without being a database expert. The following is an example of such a query:: 

   select(c for c in Customer if sum(c.orders.total_price) > 1000)

In this query, we would like to retrieve all customers with total purchases greater than 1000. ``select`` is a function provided by Pony ORM. The function receives a generator expression that helps describe the query to the database. Usually generators are executed in Python, but if such a generator is indicated inside the function ``select``, then it will be automatically translated to SQL and then executed inside the database. ``Customer`` is a entity class that is initially described when the application is created, and corresponds to a table in the database. 

Not every object-relational mapper offers such a convenient query syntax. In addition to ease of use, Pony ensures efficient work with data. Queries are translated into SQL that is executed quickly and efficiently. Depending on the DBMS, the syntax of the generating SQL may differ in order to take full advantage of the chosen database. The query code written in Python will look the same regardless of the DBMS, which ensures the application’s portability.

Pony allows any programmer to write complex and effective queries against a database, even without being an expert in SQL. At the same time, Pony does not "fight" with SQL – if a programmer needs to write a query in pure SQL, for example to call up a stored procedure, they can easily do this from within Pony. The basic objective of Pony is to simplify the development of web applications. A typical scenario in which Pony is used is generation of a dynamic web page.

Pony ORM team can be reached by the email: team (at) ponyorm.com.
