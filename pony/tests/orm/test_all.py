import unittest
import pony.orm

pony.orm.sql_debug(False)

from test_attribute import *
from test_keys import *
from test_collections import *
from test_diagram import *
from test_m2m import *
from test_mapping import *
from test_sqlast import *
from test_inheritance import *
from test_sqltranslator import *
from test_formatstyles import *
from test_attr_set_monad import *
from test_method_monad import *
from test_orderby_limit import *
from test_query_set_monad import *
from test_object_flat_monad import *
from test_converters import *
from test_string_mixin import *
from test_func_monad import *
from test_join_optimization import *
from test_one2one1 import *
from test_one2one2 import *
from test_symmetric_one2one import *
from test_symmetric_m2m import *
from test_orm_undo import *
from test_date import *
from test_raw_sql import *

if __name__ == '__main__':
    unittest.main()