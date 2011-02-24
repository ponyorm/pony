import unittest
import pony.db

pony.db.debug=False

from test_attribute import *
from test_keys import *
from test_collections import *
from test_diagram import *
from test_m2m import *
from test_mapping import *
from test_sqlast import *
from test_inheritance import *
from test_sqltranslator import *
from test_crud import *
from test_formatstyles import *
from test_attr_set_monad import *
from test_method_monad import *
from test_orderby_limit import *

if __name__ == '__main__':
    unittest.main()