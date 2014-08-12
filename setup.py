from __future__ import print_function

from distutils.core import setup
import sys

name = "pony"
version = "0.5.3"
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
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: Free for non-commercial use",
    "License :: OSI Approved :: GNU Affero General Public License v3",
    "License :: Other/Proprietary License",
    "License :: Free For Educational Use",
    "License :: Free for non-commercial use",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Topic :: Software Development :: Libraries",
    "Topic :: Database"
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

def main():
    python_version = sys.version_info
    if python_version < (2, 6) or python_version >= (2, 8):
        s = "Sorry, but %s %s requires Python version 2.6 or 2.7. You have version %s"
        print(s % (name, version, python_version.split(' ', 1)[0]))
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

if __name__ == "__main__":
    main()
