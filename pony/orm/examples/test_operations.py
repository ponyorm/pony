from pony.orm import *
from pony.orm.migrating.diagram_ops import *

def define_db_1():
    db = Database('sqlite', ':memory:')

    class Person(db.Entity):
        name = Required(str)

    return db

db = define_db_1()

op = AddAttr('Person', 'age', Required(int))
op.apply(db)

assert isinstance(db.Person.age, Required)
assert db.Person.age.py_type is int

show(db.Person)
print()

# op = AddAttr('Person', 'age', Required(int))
# op.apply(db)  # pony.orm.core.ERDiagramError: Name `age` already in use

db = define_db_1()

db.generate_mapping(check_tables=False)

op = AddAttr('Person', 'age', Required(int, unique=True))
op.apply(db)

assert isinstance(db.Person.age, Required)
assert db.Person.age.py_type is int

print db.schema.generate_create_script()

db = define_db_1()

op = AddEntity('Contact', (), [('type', Required(str)), ('value', Required(str))])
op.apply(db)

db.generate_mapping(check_tables=False)

print db.schema.generate_create_script()


db = define_db_1()
db.migration_in_progress = True

op = AddEntity('Contact', (), [('type', Required(str)), ('value', Required(str))])
op.apply(db)

op = AddRelation('Person', 'contacts', Set('Contact'), 'Contact', 'person', Required('Person'))
op.apply(db)

db.generate_mapping(check_tables=False)

print db.schema.generate_create_script()

print db.schema.tables['Contact'].foreign_keys