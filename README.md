Pony Object-Relational Mapper
=============================
[![Downloads](https://pepy.tech/badge/pony)](https://pepy.tech/project/pony) [![Downloads](https://pepy.tech/badge/pony/month)](https://pepy.tech/project/pony/month) [![Downloads](https://pepy.tech/badge/pony/week)](https://pepy.tech/project/pony/week)

Pony is an advanced object-relational mapper. The most interesting feature of Pony is its ability to write queries to the database using Python generator expressions and lambdas. Pony analyzes the abstract syntax tree of the expression and translates it into a SQL query.

Here is an example of generator query in Pony:
```python
    select(p for p in Product if p.name.startswith('A') and p.cost <= 1000)
```
Or the same query using lambda syntax:
```python
    Product.select(lambda p: p.name.startswith('A') and p.cost <= 1000 )
```
Pony works with SQLite, MySQL, PostgreSQL and Oracle databases.

Pony is an easy-to-learn and easy-to-use library. It makes your work more productive and helps to save resources. 

Main features:
* Compact entity definitions
* The concise query language
* Comprehensive error messages, showing the exact part where an error occurred in the query
* Displaying of the generated SQL in a readable format with indentation
* Ability to work with Pony interactively in a Python interpreter

All this helps the developer to focus on implementing the business logic of an application, instead of struggling with a mapper trying to understand how to get the data from the database.

See the example [here](https://github.com/ponyorm/pony/blob/orm/pony/orm/examples/estore.py)


Support Pony ORM Development
----------------------------

Pony ORM is Apache 2.0 licensed open source project. If you would like to support Pony ORM development, please consider:

[Become a backer or sponsor](https://ponyorm.org/donation.html)


Documentation
-------------

Documentation is available at [docs.ponyorm.org](https://docs.ponyorm.org). <br>
Check out the [Getting Started](https://docs.ponyorm.org/firststeps.html) page for a quick overview. <br>
The documentation source is available at [github.com/ponyorm/pony-doc](https://github.com/ponyorm/pony-doc).
Please create new documentation related issues [here](https://github.com/ponyorm/pony-doc/issues) or make a pull request with your improvements.


How PonyORM does its tricks?
----------------------------
[Here is a detailed answer](https://stackoverflow.com/a/16118756)


Online tool for database design
-------------------------------

Pony ORM also has the Entity-Relationship Diagram Editor which is a great tool for prototyping. You can create your database diagram online at [editor.ponyorm.com](https://editor.ponyorm.com), generate the database schema based on the diagram and start working with the database using declarative queries in seconds.


PonyORM community
-----------------

Please post your questions on [Stack Overflow](http://stackoverflow.com/questions/tagged/ponyorm).
Meet the PonyORM team, chat with the community members, and get your questions answered on our community [Telegram group](https://t.me/ponyorm).
Join our newsletter at [ponyorm.org](https://ponyorm.org).
Reach us on [Twitter](https://twitter.com/ponyorm).


License
-------

Pony ORM is released under the Apache 2.0 license.

Copyright (c) 2013-present Pony ORM. All rights reserved. info (at) ponyorm.org
