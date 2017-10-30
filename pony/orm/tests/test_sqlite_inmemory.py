import os.path
from unittest import TestCase

from pony.orm import *


class SqliteInMemoryTestCase(TestCase):
    def verify_with_data(self, filename):
        db = Database()

        class Car(db.Entity):
            make = Required(str)
            model = Required(str)

        db.bind('sqlite', filename, uri=True)
        db.generate_mapping(create_tables=True)

        with db_session:
            Car(make='Toyota', model='RAV4')
            commit()

            car = Car[1]

            self.assertEqual(car.make, 'Toyota')
            self.assertEqual(car.model, 'RAV4')

    def verify_file_not_created(self, filename):
        self.assertFalse(
            os.path.exists(filename),
            'File {} exists'.format(os.path.abspath(filename[5:]))
        )

    def do_job(self, filename):
        self.verify_with_data(filename)
        self.verify_file_not_created(filename)

    def test_pure_in_memory(self):
        self.do_job('file::memory:')

    def test_shared_cache(self):
        self.do_job('file::memory:?cache=shared')

    def test_named_in_memory(self):
        self.do_job('file:memdb1?mode=memory')

    def test_named_shared_cache(self):
        self.do_job('file:memdb1?mode=memory&cache=shared')

    def test_slashed_named_in_memory(self):
        self.do_job('file:/tmp/memdb1?mode=memory&cache=shared')
