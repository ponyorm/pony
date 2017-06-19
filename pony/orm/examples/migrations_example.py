from pony.orm.migrating import *

prev = ['20160317_120000_person_add_email_attribute_abcdef']

def initial_state(db):  # for initial migration only
    class Person(db.Entity):
        name = Required(str)
        age = Required(int)
        email = Required(str, unique=True)
        contacts = Set("Contact")

    class Contact(db.Entity):
        person = Required("Person")
        type = Required(str)
        value = Required(str)

operations = [
    AddAttr('Person', 'skype', Required(str, default='test')),
    AddEntity('Car', (), new_attrs=[('type', Required(str))])

]