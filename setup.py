from __future__ import print_function

from distutils.core import setup
import sys

name = "pony"
version = __import__('pony').__version__
description = "Pony Object-Relational Mapper"
long_description = """
About
=========
Pony ORM is easy to use and powerful object-relational mapper for Python.
Using Pony, developers can create and maintain database-oriented software applications
faster and with less effort. One of the most interesting features of Pony is
its ability to write queries to the database using generator expressions.
Pony then analyzes the abstract syntax tree of a generator and translates it
to its SQL equivalent.

Following is an example of a query in Pony::

    select(p for p in Product if p.name.startswith('A') and p.cost <= 1000)

Such approach simplify the code and allows a programmer to concentrate
on the business logic of the application.

Pony translates queries to SQL using a specific database dialect.
Currently Pony works with SQLite, MySQL, PostgreSQL and Oracle databases.

The package `pony.orm.examples <https://github.com/ponyorm/pony/tree/orm/pony/orm/examples>`_
contains several examples.

Installation
=================
::

    pip install pony

Entity-Relationship Diagram Editor
=============================================
`Pony online ER Diagram Editor <https://editor.ponyorm.com>`_ is a great tool for prototyping.
You can draw your ER diagram online, generate  Pony entity declarations or SQL script for
creating database schema based on the diagram and start working with the database in seconds.

Pony ORM Links:
=================
- Main site: https://ponyorm.com
- Documentation: https://docs.ponyorm.com
- GitHub: https://github.com/ponyorm/pony
- Mailing list:  http://ponyorm-list.ponyorm.com
- ER Diagram Editor: https://editor.ponyorm.com
- Blog: https://blog.ponyorm.com
"""

classifiers = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Topic :: Software Development :: Libraries',
    'Topic :: Database'
]

author = "Alexander Kozlovsky, Alexey Malashkevich"
author_email = "team@ponyorm.com"
url = "https://ponyorm.com"
licence = "Apache License Version 2.0"

packages = [
    "pony",
    "pony.orm",
    "pony.orm.dbproviders",
    "pony.orm.examples",
    "pony.orm.integration",
    "pony.orm.tests",
    "pony.thirdparty",
    "pony.thirdparty.compiler",
    "pony.utils"
]

download_url = "http://pypi.python.org/pypi/pony/"

if __name__ == "__main__":
    pv = sys.version_info[:2]
    if pv not in ((2, 7), (3, 3), (3, 4), (3, 5)):
        s = "Sorry, but %s %s requires Python of one of the following versions: 2.7, 3.3, 3.4 and 3.5." \
            " You have version %s"
        print(s % (name, version, sys.version.split(' ', 1)[0]))
        sys.exit(1)

    setup(
        name=name,
        version=version,
        description=description,
        long_description=long_description,
        classifiers=classifiers,
        author=author,
        author_email=author_email,
        url=url,
        license=licence,
        packages=packages,
        download_url=download_url
    )
