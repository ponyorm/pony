import unittest
import pony.orm.core, pony.options

pony.options.CUT_TRACEBACK = False
pony.orm.core.sql_debug(False)

from test_diagram import *
from test_diagram_attribute import *
from test_diagram_inheritance import *
from test_diagram_keys import *
from test_mapping import *
from test_relations_one2one1 import *
from test_relations_one2one2 import *
from test_relations_symmetric_one2one import *
from test_relations_symmetric_m2m import *
from test_relations_one2many import *
from test_relations_m2m import *
from test_crud_raw_sql import *
from test_declarative_attr_set_monad import *
from test_declarative_date import *
from test_declarative_func_monad import *
from test_declarative_join_optimization import *
from test_declarative_method_monad import *
from test_declarative_object_flat_monad import *
from test_declarative_orderby_limit import *
from test_declarative_string_mixin import *
from test_declarative_query_set_monad import *
from test_declarative_sqltranslator import *
from test_declarative_sqltranslator2 import *
from test_declarative_exceptions import *
from test_collections import *
from test_sqlbuilding_formatstyles import *
from test_sqlbuilding_sqlast import *
from test_orm_query import *
from test_frames import *
from test_core_multiset import *
from test_core_find_in_cache import *
from test_db_session import *

#from new_tests import *

if __name__ == '__main__':
    unittest.main()