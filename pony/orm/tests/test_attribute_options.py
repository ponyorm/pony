import unittest
from decimal import Decimal
from datetime import datetime, time
from random import randint

from pony import orm
from pony.orm.core import *
from pony.orm.tests import setup_database, teardown_database
from pony.orm.tests.testutils import raises_exception

db = Database()

class Person(db.Entity):
    id = PrimaryKey(int)
    name = orm.Required(str, 40)
    lastName = orm.Required(str, max_len=40, unique=True)
    age = orm.Optional(int, max=60, min=10)
    nickName = orm.Optional(str, autostrip=False)
    middleName = orm.Optional(str, nullable=True)
    rate = orm.Optional(Decimal, precision=11)
    salaryRate = orm.Optional(Decimal, precision=13, scale=8)
    timeStmp = orm.Optional(datetime, precision=6)
    gpa = orm.Optional(float, py_check=lambda val: val >= 0 and val <= 5)
    vehicle = orm.Optional(str, column='car')

class TestAttributeOptions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with orm.db_session:
            p1 = Person(id=1, name='Andrew', lastName='Bodroue', age=40, rate=0.980000000001, salaryRate=0.98000001)
            p2 = Person(id=2, name='Vladimir', lastName='Andrew ', nickName='vlad  ')
            p3 = Person(id=3, name='Nick', lastName='Craig', middleName=None, timeStmp='2010-12-10 14:12:09.019473',
                        vehicle='dodge')

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()
    
    def test_optionalStringEmpty(self):
        queryResult = select(p.id for p in Person if p.nickName==None).first()
        self.assertIsNone(queryResult)
        
    def test_optionalStringNone(self):
        queryResult = select(p.id for p in Person if p.middleName==None).first()
        self.assertIsNotNone(queryResult)        
    
    def test_stringAutoStrip(self):
        self.assertEqual(Person[2].lastName, 'Andrew')
        
    def test_stringAutoStripFalse(self):
        self.assertEqual(Person[2].nickName, 'vlad  ')
    
    def test_intNone(self):
        queryResult = select(p.id for p in Person if p.age==None).first()
        self.assertIsNotNone(queryResult)
    
    def test_columnName(self):
        self.assertEqual(getattr(Person.vehicle, 'column'), 'car')
        
    def test_decimalPrecisionTwo(self):
        queryResult = select(p.rate for p in Person if p.age==40).first()
        self.assertAlmostEqual(float(queryResult), 0.98, 12)
    
    def test_decimalPrecisionEight(self):
        queryResult = select(p.salaryRate for p in Person if p.age==40).first()
        self.assertAlmostEqual(float(queryResult), 0.98000001, 8)
        
    def test_fractionalSeconds(self):
        queryResult = select(p.timeStmp for p in Person if p.name=='Nick').first()
        self.assertEqual(queryResult.microsecond, 19473)
    
    def test_intMax(self):
        p4 = Person(id=4, name='Denis', lastName='Blanc', age=60)
        
    def test_intMin(self):
        p4 = Person(id=4, name='Denis', lastName='Blanc', age=10)
    
    @raises_exception(ValueError, "Value 61 of attr Person.age is greater than the maximum allowed value 60")
    def test_intMaxException(self):
        p4 = Person(id=4, name='Denis', lastName='Blanc', age=61)
     
    @raises_exception(ValueError, "Value 9 of attr Person.age is less than the minimum allowed value 10")   
    def test_intMinException(self):
        p4 = Person(id=4, name='Denis', lastName='Blanc', age=9)

    def test_py_check(self):
        p4 = Person(id=4, name='Denis', lastName='Blanc', gpa=5)
        p5 = Person(id=5, name='Mario', lastName='Gon', gpa=1)
        flush()

    @raises_exception(ValueError, "Check for attribute Person.gpa failed. Value: 6.0")
    def test_py_checkMoreException(self):        
        p6 = Person(id=6, name='Daniel', lastName='Craig', gpa=6)

    @raises_exception(ValueError, "Check for attribute Person.gpa failed. Value: -1.0")
    def test_py_checkLessException(self):        
        p6 = Person(id=6, name='Daniel', lastName='Craig', gpa=-1)
    
    @raises_exception(TransactionIntegrityError,
                      'Object Person[...] cannot be stored in the database. IntegrityError: ...')
    def test_unique(self):
        p6 = Person(id=6, name='Boris', lastName='Bodroue')
        flush()
