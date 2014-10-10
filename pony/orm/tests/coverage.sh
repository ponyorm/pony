#!/bin/sh

coverage erase

coverage-2.7 run --source pony.orm.dbapiprovider,pony.orm.dbschema,pony.orm.decompiling,pony.orm.core,pony.orm.sqlbuilding,pony.orm.sqltranslation --branch ./test_all.py

coverage-3.4 run --source pony.orm.dbapiprovider,pony.orm.dbschema,pony.orm.decompiling,pony.orm.core,pony.orm.sqlbuilding,pony.orm.sqltranslation --branch test_all.py


coverage-2.7 run -a --source pony.orm.dbapiprovider,pony.orm.dbschema,pony.orm.decompiling,pony.orm.core,pony.orm.sqlbuilding,pony.orm.sqltranslation --branch test_queries.py

coverage-3.4 run -a --source pony.orm.dbapiprovider,pony.orm.dbschema,pony.orm.decompiling,pony.orm.core,pony.orm.sqlbuilding,pony.orm.sqltranslation --branch test_queries.py

coverage html
