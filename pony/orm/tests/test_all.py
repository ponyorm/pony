from __future__ import absolute_import, print_function

import unittest
import pony.orm.core, pony.options

pony.options.CUT_TRACEBACK = False
pony.orm.core.sql_debug(False)

from pony.orm.tests.test_diagram import *
from pony.orm.tests.test_diagram_attribute import *
from pony.orm.tests.test_diagram_keys import *
from pony.orm.tests.test_mapping import *
from pony.orm.tests.test_relations_one2one1 import *
from pony.orm.tests.test_relations_one2one2 import *
from pony.orm.tests.test_relations_one2one3 import *
from pony.orm.tests.test_relations_symmetric_one2one import *
from pony.orm.tests.test_relations_symmetric_m2m import *
from pony.orm.tests.test_relations_one2many import *
from pony.orm.tests.test_relations_m2m import *
from pony.orm.tests.test_crud_raw_sql import *
from pony.orm.tests.test_declarative_attr_set_monad import *
from pony.orm.tests.test_declarative_strings import *
from pony.orm.tests.test_declarative_date import *
from pony.orm.tests.test_declarative_func_monad import *
from pony.orm.tests.test_declarative_join_optimization import *
from pony.orm.tests.test_declarative_object_flat_monad import *
from pony.orm.tests.test_declarative_orderby_limit import *
from pony.orm.tests.test_declarative_query_set_monad import *
from pony.orm.tests.test_declarative_sqltranslator import *
from pony.orm.tests.test_declarative_sqltranslator2 import *
from pony.orm.tests.test_declarative_exceptions import *
from pony.orm.tests.test_collections import *
from pony.orm.tests.test_sqlbuilding_formatstyles import *
from pony.orm.tests.test_sqlbuilding_sqlast import *
from pony.orm.tests.test_query import *
from pony.orm.tests.test_frames import *
from pony.orm.tests.test_core_multiset import *
from pony.orm.tests.test_core_find_in_cache import *
from pony.orm.tests.test_db_session import *
from pony.orm.tests.test_lazy import *
from pony.orm.tests.test_filter import *
from pony.orm.tests.test_crud import *
from pony.orm.tests.test_to_dict import *
from pony.orm.tests.test_flush import *
from pony.orm.tests.test_time_parsing import *
from pony.orm.tests.test_hooks import *
from pony.orm.tests.test_show import *
from pony.orm.tests.test_prefetching import *
from pony.orm.tests.test_indexes import *
from pony.orm.tests.test_inheritance import *

#from new_tests import *

if __name__ == '__main__':
    unittest.main()