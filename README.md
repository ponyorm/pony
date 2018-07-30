Pony Object-Relational Mapper
=============================

Pony is an advanced object-relational mapper. The most interesting feature of Pony is its ability to write queries to the database using Python generator expressions. Pony analyzes the abstract syntax tree of the generator expression and translates it into a SQL query.

Here is an example query in Pony:

    select(p for p in Product if p.name.startswith('A') and p.cost <= 1000)

Pony translates queries to SQL using a specific database dialect. Currently Pony works with SQLite, MySQL, PostgreSQL and Oracle databases.

By providing a Pythonic API, Pony facilitates fast app development. Pony is an easy-to-learn and easy-to-use library. It makes your work more productive and helps to save resources. Pony achieves this ease of use through the following:

* Compact entity definitions
* The concise query language
* Ability to work with Pony interactively in a Python interpreter
* Comprehensive error messages, showing the exact part where an error occurred in the query
* Displaying of the generated SQL in a readable format with indentation

All this helps the developer to focus on implementing the business logic of an application, instead of struggling with a mapper trying to understand how to get the data from the database.

See the example [here](https://github.com/ponyorm/pony/blob/orm/pony/orm/examples/estore.py)


Online tool for database design
-------------------------------

Pony ORM also has the Entity-Relationship Diagram Editor which is a great tool for prototyping. You can create your database diagram online at [https://editor.ponyorm.com](https://editor.ponyorm.com), generate the database schema based on the diagram and start working with the database using declarative queries in seconds.


Documentation
-------------

Documenation is available at [https://docs.ponyorm.com](https://docs.ponyorm.com)
The documentation source is avaliable at [https://github.com/ponyorm/pony-doc](https://github.com/ponyorm/pony-doc).
Please create new documentation related issues [here](https://github.com/ponyorm/pony-doc/issues) or make a pull request with your improvements.


License
-------

Pony ORM is released under the Apache 2.0 license.


PonyORM community
-----------------

Please post your questions on [Stack Overflow](http://stackoverflow.com/questions/tagged/ponyorm).
Meet the PonyORM team, chat with the community members, and get your questions answered on our community [Telegram group](https://telegram.me/ponyorm).
Join our newsletter at [ponyorm.com](https://ponyorm.com).
Reach us on [Twitter](https://twitter.com/ponyorm).

Copyright (c) 2018 Pony ORM, LLC. All rights reserved. team (at) ponyorm.com
