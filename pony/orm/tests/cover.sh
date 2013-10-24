coverage erase
coverage run --source pony.orm.dbapiprovider,pony.orm.dbschema,pony.orm.decompiling,pony.orm.core,pony.orm.sqlbuilding,pony.orm.sqltranslation --branch test_all.py
coverage run -a --source pony.orm.dbapiprovider,pony.orm.dbschema,pony.orm.decompiling,pony.orm.core,pony.orm.sqlbuilding,pony.orm.sqltranslation --branch test_queries.py
coverage html
