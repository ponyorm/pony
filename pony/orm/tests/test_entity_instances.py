import unittest

from pony import orm
from pony.orm.core import *
from pony.orm.tests.testutils import raises_exception

db = Database('sqlite', ':memory:')

class Person(db.Entity):
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str, 40)
    lastName = orm.Required(str, max_len=40, unique=True)
    age = orm.Optional(int)
    groupName = orm.Optional('Group')
    chiefOfGroup = orm.Optional('Group')

class Group(db.Entity):
    name = orm.Required(str)
    persons = orm.Set(Person)
    chief = orm.Optional(Person, reverse='chiefOfGroup')

db.generate_mapping(create_tables=True)

class TestEntityInstances(unittest.TestCase):
    
    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
       rollback()
       db_session.__exit__()
    
    def test_create_instance(self):
        with orm.db_session:
            Person(id=1, name='Philip', lastName='Croissan')
            Person(id=2, name='Philip', lastName='Parlee', age=40)
            Person(id=3, name='Philip', lastName='Illinois', age=50)
            commit()
    
    def test_getObjectByPK(self):
        self.assertEqual(Person[1].lastName, "Croissan")
    
    @raises_exception(ObjectNotFound , "Person[666]")
    def test_getObjectByPKexception(self):
        p = Person[666]
        
    def test_getObjectByGet(self):
        p = Person.get(age=40)
        self.assertEqual(p.lastName, "Parlee")
    
    def test_getObjectByGetNone(self):
        self.assertIsNone(Person.get(age=41))
    
    @raises_exception(MultipleObjectsFoundError , 'Multiple objects were found.'
                       ' Use Person.select(...) to retrieve them')
    def test_getObjectByGetException(self):
        p = Person.get(name="Philip")
    
    def test_updateObject(self):
        with db_session:
            Person[2].age=42
        self.assertEqual(Person[2].age, 42)
        commit()

    @raises_exception(ObjectNotFound, 'Person[2]')
    def test_deleteObject(self):
        with db_session:
            Person[2].delete()
        p = Person[2]
    
    def test_bulkDelete(self):
        with orm.db_session:
            Person(id=4, name='Klaus', lastName='Mem', age=12)
            Person(id=5, name='Abraham', lastName='Wrangler', age=13)
            Person(id=6, name='Kira', lastName='Phito', age=20)
            delete(p for p in Person if p.age <= 20)
        self.assertEqual(select(p for p in Person if p.age <= 20).count(), 0)
        
    def test_bulkDeleteV2(self):
        with orm.db_session:
            Person(id=4, name='Klaus', lastName='Mem', age=12)
            Person(id=5, name='Abraham', lastName='Wrangler', age=13)
            Person(id=6, name='Kira', lastName='Phito', age=20)
            Person.select(lambda p: p.id >= 4).delete(bulk=True)
        self.assertEqual(select(p for p in Person if p.id >= 4).count(), 0)
    
    @raises_exception(UnresolvableCyclicDependency, 'Cannot save cyclic chain: Person -> Group')  
    def test_saveChainsException(self):
        with orm.db_session:
            claire = Person(name='Claire', lastName='Forlani')
            annabel = Person(name='Annabel', lastName='Fiji')
            Group(name='Aspen', persons=[claire, annabel], chief=claire)
        print('group1=', Group[1])
    
    def test_saveChainsWithFlush(self):
        with orm.db_session:
            claire = Person(name='Claire', lastName='Forlani')
            annabel = Person(name='Annabel', lastName='Fiji')
            flush()
            Group(name='Aspen', persons=[claire, annabel], chief=claire)
        self.assertEqual(Group[1].name, 'Aspen')
        self.assertEqual(Group[1].chief.lastName, 'Forlani')