import re
import sys
from pathlib import Path

from setuptools import setup


name = "pony"
version_file = Path(__file__).parent / "pony" / "__init__.py"
version_match = re.search(
    r'^__version__ = ["\']([^"\']+)["\']',
    version_file.read_text(encoding="utf-8"),
    re.MULTILINE,
)
if version_match is None:
    raise RuntimeError("Cannot find Pony version")
version = version_match.group(1)
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
Currently Pony works with SQLite, MySQL, PostgreSQL, Oracle and CockroachDB databases.

The package `pony.orm.examples <https://github.com/ponyorm/pony/tree/orm/pony/orm/examples>`_
contains several examples.

Installation
=================
::

    pip install pony
    pip install "pony[mysql]"
    pip install "pony[postgres]"
    pip install "pony[oracle]"
    pip install "pony[cockroach]"
    pip install "pony[async-sqlite]"
    pip install "pony[async-postgres]"

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
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
    "Programming Language :: Python :: Implementation :: PyPy",
    "Topic :: Software Development :: Libraries",
    "Topic :: Database",
]

author = ", ".join(
    [
        "Alexander Kozlovsky <alexander.kozlovsky@gmail.com>",
        "Alexey Malashkevich <alexey@ponyorm.org>",
        "Alexander Tischenko <sashaaero@yandex.ru>",
    ]
)
author_email = "team@ponyorm.com"
url = "https://ponyorm.com"
project_urls = {
    "Documentation": "https://docs.ponyorm.org",
    "Source": "https://github.com/ponyorm/pony",
}
packages = [
    "pony",
    "pony.flask",
    "pony.flask.example",
    "pony.orm",
    "pony.orm.asyncio",
    "pony.orm.dbproviders",
    "pony.orm.examples",
    "pony.orm.integration",
    "pony.orm.tests",
    "pony.thirdparty",
    "pony.utils",
]

install_requires = [
    "greenlet>=3.5.4,<4",
]

extras_require = {
    "mysql": [
        "PyMySQL[rsa]>=1.2,<2",
    ],
    "postgres": [
        "psycopg2>=2.9.11,<3",
    ],
    "oracle": [
        "oracledb>=4.0.2,<5",
    ],
    "cockroach": [
        "psycopg2>=2.9.11,<3",
    ],
    "async-sqlite": [
        "aiosqlite>=0.22.1,<1",
    ],
    "async-postgres": [
        "asyncpg>=0.31,<1",
    ],
}

package_data = {
    "pony.flask.example": ["templates/*.html"],
    "pony.orm.tests": ["queries.txt"],
}

download_url = "http://pypi.python.org/pypi/pony/"

if __name__ == "__main__":
    pv = sys.version_info[:2]
    if pv < (3, 10) or pv > (3, 14):
        s = (
            "Sorry, but %s %s requires Python of one of the following versions: 3.10-3.14."
            " You have version %s"
        )
        print(s % (name, version, sys.version.split(" ", 1)[0]))
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
        project_urls=project_urls,
        packages=packages,
        package_data=package_data,
        install_requires=install_requires,
        extras_require=extras_require,
        download_url=download_url,
        python_requires=">=3.10,<3.15",
    )
