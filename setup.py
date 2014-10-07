from __future__ import print_function

from distutils.core import setup
import sys

name = "pony"
version = __import__('pony').__version__
description = "Pony Object-Relational Mapper"
long_description = """
Pony is an object-relational mapper. The most interesting feature of Pony is
its ability to write queries to the database using generator expressions.
Pony works with entities which are mapped to a SQL database. Using generator
syntax for writing queries allows the user to formulate very eloquent queries.
It increases the level of abstraction and allows a programmer to concentrate
on the business logic of the application. For this purpose Pony analyzes the
abstract syntax tree of a generator and translates it to its SQL equivalent.

Following is an example of a query in Pony:

    select(p for p in Product if p.name.startswith('A') and p.cost <= 1000)

Pony translates queries to SQL using a specific database dialect.
Currently Pony works with SQLite, MySQL, PostgreSQL and Oracle databases.

The package pony.orm.examples contains several examples.
Documenation is available at http://ponyorm.com"""

classifiers = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: Free for non-commercial use',
    'License :: OSI Approved :: GNU Affero General Public License v3',
    'License :: Other/Proprietary License',
    'License :: Free For Educational Use',
    'License :: Free for non-commercial use',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Topic :: Software Development :: Libraries',
    'Topic :: Database'
]

author = "Alexander Kozlovsky, Alexey Malashkevich"
author_email = "team@ponyorm.com"
url = "http://ponyorm.com"
lic = "AGPL, Commercial, Free for educational and non-commercial use"

packages = [
    "pony",
    "pony.orm",
    "pony.orm.dbproviders",
    "pony.orm.examples",
    "pony.orm.integration",
    "pony.orm.tests",
    "pony.thirdparty",
    "pony.thirdparty.compiler"
]

download_url = "http://pypi.python.org/pypi/pony/"

if __name__ == "__main__":
    pv = sys.version_info[:2]
    if pv not in ((2, 6), (2, 7), (3, 3), (3, 4)):
        s = "Sorry, but %s %s requires Python of one of the following versions: 2.6, 2.7, 3.3 or 3.4." \
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
        license=lic,
        packages=packages,
        download_url=download_url
    )
