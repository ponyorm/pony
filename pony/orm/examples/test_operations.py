from __future__ import print_function
from pony.orm import *
from pony.migrate import diagram_ops as ops

def define_db_1():
    db = Database('sqlite', ':memory:')

    class Person(db.Entity):
        name = Required(str)

    return db

db = define_db_1()

op = ops.AddAttr('Person', 'age', Required(int))
op.apply(db)

assert isinstance(db.Person.age, Required)
assert db.Person.age.py_type is int

show(db.Person)
print()

# op = AddAttr('Person', 'age', Required(int))
# op.apply(db)  # pony.orm.core.ERDiagramError: Name `age` already in use

db = define_db_1()

db.generate_mapping(check_tables=False)

op = ops.AddAttr('Person', 'age', Required(int, unique=True))
op.apply(db)

assert isinstance(db.Person.age, Required)
assert db.Person.age.py_type is int

print(db.schema.generate_create_script())

db = define_db_1()

op = ops.AddEntity('Contact', (), [('type', Required(str)), ('value', Required(str))])
op.apply(db)

db.generate_mapping(check_tables=False)

print(db.schema.generate_create_script())


db = define_db_1()
db.migration_in_progress = True

op = ops.AddEntity('Contact', (), [('type', Required(str)), ('value', Required(str))])
op.apply(db)


op = ops.AddRelation('Person', 'contacts', Set('Contact'), 'person', Required('Person'))
op.apply(db)

db.generate_mapping(check_tables=False)

print(db.schema.tables['contact'].foreign_keys)
