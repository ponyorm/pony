from distutils.core import setup
import sys

name = "pony"
version = "0.3"
description = "Pony Object-Relational Mapper"
long_description = """Pony helps to simplify data management. Using Pony you can work 
with the data in terms of entities and their relationships. 
Pony also allows querying data in pure Python using the syntax of generator 
expressions."""

classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: Free for non-commercial use",
    "License :: OSI Approved :: GNU Affero General Public License v3",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Topic :: Software Development :: Libraries",
    "Topic :: Database"
]

author = "Pony Team"
author_email = "team@ponyorm.com"
url = "http://ponyorm.com"
lic = "AGPL"

modules = [
    "pony.__init__",
    "pony.converting",
    "pony.options",
    "pony.utils"
]

packages = [
    "pony.orm",
    "pony.orm.dbproviders",
    "pony.orm.examples",
    "pony.orm.tests"
]

download_url = "http://pypi.python.org/pypi/pony/"

def main():
    python_version = sys.version_info
    if python_version < (2, 5) or python_version >= (2, 8):
        s = "Sorry, but %s %s requires Python version 2.5, 2.6 or 2.7. You have version %s"
        print s % (name, version, python_version.split(' ', 1)[0])
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
        download_url=download_url,
        py_modules=modules
    )

if __name__ == "__main__":
    main()
