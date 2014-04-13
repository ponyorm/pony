import unittest

from testutils import *
from model1 import *

class TestQuery(unittest.TestCase):
    @raises_exception(TypeError, 'Cannot iterate over non-entity object')
    def test_query_1(self):
        select(s for s in [])
    @raises_exception(TypeError, 'Cannot iterate over non-entity object X')
    def test_query_2(self):
        X = [1, 2, 3]
        select('x for x in X')
