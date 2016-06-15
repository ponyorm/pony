# coding: utf-8

from pony.py23compat import PY2

CONN_STRING = (
    'DSN=MSSQLdb;'
    'SERVER=mssql;'
    'DATABASE=db1;'
    'UID=sa;'
    'PWD=pass'
)
from pony.orm import *
from pony import options

import os

options.CUT_TRACEBACK = False

def getdb():
    return Database('mssqlserver', CONN_STRING)

def get_mysql_db():
    return Database('mysql', host="localhost", user="root",
                    passwd="muscul", db="mydb")

def test_db():
    getdb()


from pony.py23compat import buffer


import unittest
from pony.testing import TestCase

from .util import *

from pony.langutils import cached_property, class_property

class TestSetup(object):
    def __init__(self, cls):
        self.test_cls = cls

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @cached_property
    def db_name(self):
        import random
        return 'd%s' % str(random.random())[2:]

    @classmethod
    def as_mixin(cls):
        class C(object):
            case = None

            @class_property
            def db(test_cls):
                return test_cls.case.db if test_cls.case else None

            @classmethod
            def make_entities(test_cls):
                pass

            @classmethod
            def setUpClass(test_cls):
                self = test_cls.case = cls(test_cls)
                self.setUp()
                test_cls.make_entities()
                test_cls.db.generate_mapping(check_tables=True, create_tables=True)

            @classmethod
            def tearDownClass(test_cls):
                self = test_cls.case
                self.tearDown()

        name = '%sMixin' % cls.__name__
        return type(name, (object,), dict(C.__dict__))

from contextlib import closing


class MySqlSetup(TestSetup):

    def setUp(self):
        from pony.orm.dbproviders.mysql import mysql_module
        with closing(mysql_module.connect(**self.CONN).cursor()) as c:
            c.execute('create database %s' % self.db_name)
            c.execute('use %s' % self.db_name)

    CONN = {
        'host': "localhost",
        'user': "root",
        'passwd': "muscul",
    }

    def tearDown(self):
        from pony.orm.dbproviders.mysql import mysql_module
        with closing(mysql_module.connect(**self.CONN).cursor()) as c:
            c.execute('use mydb')
            c.execute('drop database %s' % self.db_name)

    @cached_property
    def db(self):
        CONN = dict(self.CONN, db=self.db_name)
        return Database('mysql', **CONN)


class MSSQLSetup(TestSetup):

    def setUp(self):
        import pyodbc
        cursor = pyodbc.connect(self.get_conn_string(), autocommit=True).cursor()
        try:
            cursor.execute('create database %s' % self.db_name)
            cursor.execute('use %s' % self.db_name)
        finally:
            cursor.close()


    def get_conn_string(self, db=None):
        s = (
            'DSN=MSSQLdb;'
            'SERVER=mssql;'
            'UID=sa;'
            'PWD=pass;'
        )
        if db:
            s += 'DATABASE=%s' % db
        return s

    @cached_property
    def db(self):
        CONN = self.get_conn_string(self.db_name)
        return Database('mssqlserver', CONN)
        # return get_mysql_db()

    def tearDown(self):
        CONN = self.get_conn_string()
        import pyodbc
        cursor = pyodbc.connect(CONN, autocommit=True).cursor()
        try:
            cursor.execute('use master')
            cursor.execute('drop database %s' % self.db_name)
        finally:
            cursor.close()


class SqliteSetup(TestSetup):
    def tearDown(self):
        os.remove(self.db_path)

    @cached_property
    def db_path(self):
        p = os.path.dirname(__file__)
        p = os.path.join(p, self.db_name)
        return os.path.abspath(p)

    @cached_property
    def db(self):
        return Database('sqlite', self.db_path, create_db=True)





class Test1(MSSQLSetup.as_mixin(), TestCase):

    @classmethod
    def make_entities(cls):
        class E(cls.db.Entity):
            msg = Required(str)

        class HasInt(cls.db.Entity):
            msg = Optional(str)
            i = Required(int)

    @classmethod
    def setUpClass(cls):
        super(Test1, cls).setUpClass()
        db = cls.db
        with db_session:
            msg = db.E(msg='hello world')
            o = db.HasInt(i=10, msg='hello world')

    @db_session
    def test_first(self):
        qu = select(
            o.msg for o in self.db.E
            if o.msg.startswith('h')
        )
        self.assertEqual(qu.first(), 'hello world')

    @db_session
    def test_offset(self):
        items = self.db.E.select()[0:1]
        self.assertTrue(items)

    @db_session
    def test_order_by(self):
        def f(e):
            return e.msg
        first = self.db.E.select().order_by(f).first()
        self.assertTrue(first)


    @db_session
    def test_simple(self):
        qu = select(
            o.msg for o in self.db.E
            if o.msg.startswith('h')
        )
        self.assertSequenceEqual(qu[:], ['hello world'])

    @db_session
    def test_if(self):
        qu = select(
            o.msg for o in self.db.HasInt
            if o.i * 100 > o.id and o.msg.startswith('h')
        )
        self.assertTrue(qu.count())


from datetime import datetime

class TestsCreate(MSSQLSetup.as_mixin(), TestCase):

    @classmethod
    def make_entities(cls):
        db = cls.db

        class Location(db.Entity):
            id = PrimaryKey(int, auto=True)
            Name = Required(str, unique=True)
            children = Set("Location", reverse="parent", cascade_delete=True)
            parent = Optional("Location", reverse="children")
            node = Optional("Node", cascade_delete=True)

        class Node(db.Entity):
            id = PrimaryKey(int, auto=True)
            location = Optional(Location)
            Enabled = Required(bool)
            Cores = Required(int)
            Start_date = Required(datetime)


    def test(self):
        with db_session:
            loc_root = self.db.Location(Name='root')
            node_br00 = self.db.Node(location=loc_root, Enabled=True, Cores=1, Start_date=datetime(2015, 12, 1))
        with db_session:
            no = select(n for n in self.db.Node).first()
            self.assertTrue(no.location)


class TestLocationsAndNodes(MSSQLSetup.as_mixin(), TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestLocationsAndNodes, cls).setUpClass()
        with db_session:
            db_model = cls.db
            loc_root = db_model.Location(Name='root')
            loc_br0 = db_model.Location(Name='br0', parent=loc_root)
            loc_br1 = db_model.Location(Name='br1', parent=loc_root)
            loc_br00 = db_model.Location(Name='br00', parent=loc_br0)
            loc_br01 = db_model.Location(Name='br01', parent=loc_br0)
            loc_br10 = db_model.Location(Name='br10', parent=loc_br1)
            loc_br11 = db_model.Location(Name='br11', parent=loc_br1)

            #create 3 nodes in br00, 01, 10
            node_br00 = db_model.Node(location=loc_br00, Enabled=True, Cores=1, Start_date=datetime(2015, 12, 1))
            node_br01 = db_model.Node(location=loc_br01, Enabled=True, Cores=4, Start_date=datetime(2016, 1, 1))
            node_br10 = db_model.Node(location=loc_br10, Enabled=True, Cores=4, Start_date=datetime(2016, 2, 1))


    @classmethod
    def make_entities(cls):
        db = cls.db

        class Location(db.Entity):
            id = PrimaryKey(int, auto=True)
            Name = Required(str, unique=True)
            children = Set("Location", reverse="parent", cascade_delete=True)
            parent = Optional("Location", reverse="children")
            node = Optional("Node", cascade_delete=True)

        class Node(db.Entity):
            id = PrimaryKey(int, auto=True)
            location = Optional(Location)
            Enabled = Required(bool)
            Cores = Required(int)
            Start_date = Required(datetime)

    @db_session
    def test_one_to_one(self):
        have_nodes = [
            'br00', 'br01', 'br10'
        ]
        for node in self.db.Node.select():
            self.assertTrue(node.location)
        for loc in self.db.Location.select():
            if loc.Name in have_nodes:
                self.assertTrue(loc.node)
            else:
                self.assertFalse(loc.node)


    @db_session
    def test_set(self):
        have_children = [
            'root', 'br0', 'br1'
        ]
        for loc in self.db.Location.select():
            if loc.Name in have_children:
                self.assertTrue(loc.children)
            else:
                self.assertFalse(loc.children)
                self.assertTrue(loc.parent)




class TestMore(MySqlSetup.as_mixin(), TestCase):

    @classmethod
    def make_entities(cls):
        db = cls.db

        class Location(db.Entity):
            id = PrimaryKey(int, auto=True)
            Name = Required(str, unique=True)
            children = Set("Location", reverse="parent", cascade_delete=True)
            parent = Optional("Location", reverse="children")
            node = Optional("Node", cascade_delete=True)

        class Node(db.Entity):
            id = PrimaryKey(int, auto=True)
            location = Optional(Location)
            node_settings = Optional("Node_settings", cascade_delete=True)
            Enabled = Required(bool)
            Cores = Required(int)
            Start_date = Required(datetime)
            node_logs = Set("Node_log", cascade_delete=True)
            node_profile = Required("Node_profile")

        class Node_profile(db.Entity):
            id = PrimaryKey(int, auto=True)
            Name = Required(str, unique=True)
            Color = Optional(LongStr)
            nodes = Set(Node, cascade_delete=True)
            HTML_page = Required(buffer)

        class Node_settings(db.Entity):
            id = PrimaryKey(int, auto=True)
            IP_Address = Required(str)
            IP_Netmask = Required(unicode)
            node = Required(Node)
            HW_version = Required(str)
            Display_resolution = Required(str)
            Display_wallpaper = Required(buffer)

        class Node_log(db.Entity):
            id = PrimaryKey(int, auto=True)
            Timestamp = Required(datetime)
            Location_name = Optional(str)
            Event = Required(str)
            Data = Optional(LongStr)
            node = Optional(Node)

    @classmethod
    def setUpClass(cls):
        super(TestMore, cls).setUpClass()
        with db_session:
            db_model = cls.db
            #create root location and 2 branches, each with 2 more children - 7 locations in total
            loc_root = db_model.Location(Name='root')
            loc_br0 = db_model.Location(Name='br0', parent=loc_root)
            loc_br1 = db_model.Location(Name='br1', parent=loc_root)
            loc_br00 = db_model.Location(Name='br00', parent=loc_br0)
            loc_br01 = db_model.Location(Name='br01', parent=loc_br0)
            loc_br10 = db_model.Location(Name='br10', parent=loc_br1)
            loc_br11 = db_model.Location(Name='br11', parent=loc_br1)

            #create 2 node profiles
            node_prof_default = db_model.Node_profile(Name='Default', Color='white', HTML_page=html_page_default)
            node_prof_custom = db_model.Node_profile(Name='Custom', Color='white', HTML_page=html_page_custom)

            #create 3 nodes in br00, 01, 10
            node_br00 = db_model.Node(location=loc_br00, Enabled=True, Cores=1, Start_date=datetime(2015, 12, 1)
                            , node_profile=node_prof_default)
            node_br01 = db_model.Node(location=loc_br01, Enabled=True, Cores=4, Start_date=datetime(2016, 1, 1)
                            , node_profile=node_prof_custom)
            node_br10 = db_model.Node(location=loc_br10, Enabled=True, Cores=4, Start_date=datetime(2016, 2, 1)
                            , node_profile=node_prof_default)

            #create node settings for all
            node_set_00 = db_model.Node_settings(IP_Address='1.2.3.4', IP_Netmask='255.255.0.0'
                                        , HW_version='B01', Display_resolution='800x600'
                                        , Display_wallpaper=wallpaper, node=node_br00)
            node_set_01 = db_model.Node_settings(IP_Address='1.2.3.5', IP_Netmask='255.255.0.0'
                                        , HW_version='B02', Display_resolution='800x600'
                                        , Display_wallpaper=wallpaper, node=node_br01)
            node_set_10 = db_model.Node_settings(IP_Address='1.2.3.6', IP_Netmask='255.255.0.0'
                                        , HW_version='B03', Display_resolution='1024x768'
                                        , Display_wallpaper=wallpaper, node=node_br10)

            #create 10 log entries for the 3 nodes
            node_log_000 = db_model.Node_log(Timestamp=datetime(2015, 12, 1, 0, 0, 10), Location_name='br00'
                                , Event='Startup', Data='', node=node_br00)
            node_log_010 = db_model.Node_log(Timestamp=datetime(2016, 1, 1, 0, 0, 10), Location_name='br01'
                                , Event='Startup', Data='', node=node_br01)
            node_log_100 = db_model.Node_log(Timestamp=datetime(2016, 2, 1, 0, 0, 10), Location_name='br10'
                                , Event='Startup', Data='', node=node_br10)
            node_log_001 = db_model.Node_log(Timestamp=datetime(2015, 12, 1, 2, 0, 10), Location_name='br00'
                                , Event='Rain start', Data='', node=node_br00)
            node_log_002 = db_model.Node_log(Timestamp=datetime(2015, 12, 1, 2, 0, 20), Location_name='br00'
                                , Event='Temp measure', Data='23.5', node=node_br00)
            node_log_003 = db_model.Node_log(Timestamp=datetime(2015, 12, 1, 2, 5, 20), Location_name='br00'
                                , Event='Rain L/m2/h', Data='0.2', node=node_br00)
            node_log_004 = db_model.Node_log(Timestamp=datetime(2015, 12, 1, 2, 5, 40), Location_name='br00'
                                , Event='Temp measure', Data='23.0', node=node_br00)
            node_log_005 = db_model.Node_log(Timestamp=datetime(2015, 12, 1, 5, 34, 25), Location_name='br00'
                                , Event='Rain stop', Data='', node=node_br00)
            node_log_006 = db_model.Node_log(Timestamp=datetime(2015, 12, 1, 5, 40, 20), Location_name='br00'
                                , Event='Rain L/m2/h', Data='0.48', node=node_br00)
            node_log_007 = db_model.Node_log(Timestamp=datetime(2015, 12, 1, 5, 40, 40), Location_name='br00'
                                , Event='Temp measure', Data='21.5', node=node_br00)

    @db_session
    def test01_html_page(self):
        node = select(n for n in self.db.Node).first()
        self.assertIsInstance(node.node_profile.HTML_page, buffer)

    def check_attrs(self, obj, attr_val_dict=None):
        self.assertIsNotNone(obj)
        if attr_val_dict is None:
            return None
        for attr_name, value in attr_val_dict.items():
            to_check = getattr(obj, attr_name)
            if PY2 and isinstance(to_check, buffer) and isinstance(value, str):
                to_check = str(to_check)
            try:
                self.assertEqual(to_check, value)
            except:
                print(obj, attr_name)
                raise

    def test02_create(self):
        db_model = self.db
        #create 1 more location, node_profile, node, node_setting, node_log
        with db_session:
            loc_br1 = db_model.Location.get(Name='br1')  # @UndefinedVariable
            loc_br12 = db_model.Location(Name='br12', parent=loc_br1)

            node_prof_custom2 = db_model.Node_profile(Name='Custom2', Color='green', HTML_page=html_page_custom)

            node_br12 = db_model.Node(location=loc_br12, Enabled=True, Cores=4, Start_date=datetime(2016, 5, 1)
                             , node_profile=node_prof_custom2)

            node_set_12 = db_model.Node_settings(IP_Address='1.2.3.7', IP_Netmask='255.255.0.0'
                                        , HW_version='B03', Display_resolution='1024x768'
                                        , Display_wallpaper=wallpaper, node=node_br12)

            node_log_120 = db_model.Node_log(Timestamp=datetime(2016, 5, 1, 0, 0, 10), Location_name='br12'
                                   , Event='Startup', Data='', node=node_br12)
            node_log_121 = db_model.Node_log(Timestamp=datetime(2016, 5, 1, 0, 0, 50), Location_name='br12'
                                   , Event='Temp measure', Data='23.0', node=node_br12)

        with db_session:
            loc_br1 = db_model.Location.get(Name='br1')  # @UndefinedVariable
            self.check_attrs(loc_br1)
            loc_br12 = db_model.Location.get(Name='br12')  # @UndefinedVariable
            self.check_attrs(loc_br12)
            self.assertIs(loc_br12.parent, loc_br1)

            node_prof_custom2 = db_model.Node_profile.get(Name='Custom2')  # @UndefinedVariable
            self.check_attrs(node_prof_custom2, {'Name':'Custom2', 'Color':'green', 'HTML_page':html_page_custom})

            node_br12 = loc_br12.node
            self.check_attrs(node_br12, {'Enabled':True, 'Cores':4, 'Start_date':datetime(2016, 5, 1)})
            self.assertIs(node_br12.location, loc_br12)
            self.assertIs(node_br12.node_profile, node_prof_custom2)

            node_logs = select(node_log for node_log in db_model.Node_log if node_log.node.location.Name == 'br12')
            self.assertEqual(set([node_log.Event for node_log in node_logs]), set(('Startup', 'Temp measure')))
            node_logs = {node_log.Event:node_log for node_log in node_logs}

            self.check_attrs(node_logs['Startup'],{'Timestamp':datetime(2016, 5, 1, 0, 0, 10), 'Location_name':'br12'
                                   , 'Event':'Startup', 'Data':''})
            self.assertIs(node_logs['Startup'].node, node_br12)
            self.check_attrs(node_logs['Temp measure'],{'Timestamp':datetime(2016, 5, 1, 0, 0, 50), 'Location_name':'br12'
                                   , 'Event':'Temp measure', 'Data':'23.0'})
            self.assertIs(node_logs['Temp measure'].node, node_br12)


    @db_session
    def test_FAILING_attr_is_obj(self):
        raise unittest.SkipTest('Fails on sqlite and mysql too')
        # prepare
        db_model = self.db
        loc_br1 = db_model.Location.get(Name='br1')  # @UndefinedVariable
        loc_br12 = db_model.Location(Name='br12', parent=loc_br1)

        node_prof_custom2 = db_model.Node_profile(Name='Custom2', Color='green', HTML_page=html_page_custom)

        node_br12 = db_model.Node(location=loc_br12, Enabled=True, Cores=4, Start_date=datetime(2016, 5, 1)
                            , node_profile=node_prof_custom2)
        node_log_120 = db_model.Node_log(Timestamp=datetime(2016, 5, 1, 0, 0, 10), Location_name='br12'
                                , Event='Startup', Data='', node=node_br12)
        node_log_121 = db_model.Node_log(Timestamp=datetime(2016, 5, 1, 0, 0, 50), Location_name='br12'
                                , Event='Temp measure', Data='23.0', node=node_br12)
        #
        loc_br12 = db_model.Location.get(Name='br12')  # @UndefinedVariable
        node_br12 = loc_br12.node
        log = select(
            node_log for node_log in self.db.Node_log
            if node_log.node is node_br12
        ).first()
        self.assertTrue(log)



    @db_session
    def test03_read_traverse(self):
        db_model = self.db




        #read locations and corresponding nodes, node_profiles, node_setings, node_logs
        locations = select(location for location in db_model.Location)
        locations_tree = build_locations_tree(locations)
        self.assertEqual(set(locations_tree.keys()), {'root', 'br0', 'br1', 'br00', 'br01', 'br10', 'br11', 'br12'})
        self.assertIn(locations_tree['br0'].value, locations_tree['root'].value.children)
        self.assertIn(locations_tree['br1'].value, locations_tree['root'].value.children)
        self.assertIn(locations_tree['br00'].value, locations_tree['br0'].value.children)
        self.assertIn(locations_tree['br01'].value, locations_tree['br0'].value.children)
        self.assertIn(locations_tree['br10'].value, locations_tree['br1'].value.children)
        self.assertIn(locations_tree['br11'].value, locations_tree['br1'].value.children)

        nodes = [location.node for location in locations if location.node is not None]
        node_br00 = [node for node in nodes if node.location is locations_tree['br00'].value][0]
        node_br01 = [node for node in nodes if node.location is locations_tree['br01'].value][0]
        node_br10 = [node for node in nodes if node.location is locations_tree['br10'].value][0]
        self.check_attrs(node_br00, {'Enabled':True, 'Cores':1, 'Start_date':datetime(2015, 12, 1)})
        self.check_attrs(node_br01, {'Enabled':True, 'Cores':4, 'Start_date':datetime(2016, 1, 1)})
        self.check_attrs(node_br10, {'Enabled':True, 'Cores':4, 'Start_date':datetime(2016, 2, 1)})

        node_prof_default = db_model.Node_profile.get(Name='Default')  # @UndefinedVariable
        self.assertIs(node_prof_default, node_br00.node_profile)
        self.assertIs(node_prof_default, node_br10.node_profile)
        node_prof_custom = db_model.Node_profile.get(Name='Custom')  # @UndefinedVariable
        self.assertIs(node_prof_custom, node_br01.node_profile)

        node_set_00 = node_br00.node_settings
        node_set_01 = node_br01.node_settings
        node_set_10 = node_br10.node_settings
        self.check_attrs(node_set_00, {'IP_Address':'1.2.3.4', 'IP_Netmask':'255.255.0.0'
                                        , 'HW_version':'B01', 'Display_resolution':'800x600', 'Display_wallpaper':wallpaper})
        self.check_attrs(node_set_01, {'IP_Address':'1.2.3.5', 'IP_Netmask':'255.255.0.0'
                                        , 'HW_version':'B02', 'Display_resolution':'800x600', 'Display_wallpaper':wallpaper})
        self.check_attrs(node_set_10, {'IP_Address':'1.2.3.6', 'IP_Netmask':'255.255.0.0'
                                        , 'HW_version':'B03', 'Display_resolution':'1024x768', 'Display_wallpaper':wallpaper})
        self.assertIs(node_set_00.node, node_br00)
        self.assertIs(node_set_01.node, node_br01)
        self.assertIs(node_set_10.node, node_br10)

        node_logs = [(node_log.Event, node_log.node.location.Name, node_log.Location_name)
                        for node in nodes for node_log in node.node_logs]
        for event, loc_name1, loc_name2 in node_logs:
            self.assertEqual(loc_name1, loc_name2)



    def test04_update_traverse(self):
        db_model = self.db
        with db_session:
            location_child = db_model.Location.get(Name='br1')  # @UndefinedVariable
            location_parent = db_model.Location.get(Name='br0')  # @UndefinedVariable
            location_child.parent = location_parent

        with db_session:
            locations = select(location for location in db_model.Location)
            locations_tree = build_locations_tree(locations)
            self.assertEqual(set(locations_tree.keys()), {'root', 'br0', 'br1', 'br00', 'br01', 'br10', 'br11', 'br12'})
            self.assertIn(locations_tree['br0'].value, locations_tree['root'].value.children)
            self.assertIn(locations_tree['br1'].value, locations_tree['br0'].value.children)
            self.assertIn(locations_tree['br00'].value, locations_tree['br0'].value.children)
            self.assertIn(locations_tree['br01'].value, locations_tree['br0'].value.children)
            self.assertIn(locations_tree['br10'].value, locations_tree['br1'].value.children)
            self.assertIn(locations_tree['br11'].value, locations_tree['br1'].value.children)

        with db_session:
            loc_br00 = db_model.Location.get(Name='br00')  # @UndefinedVariable
            loc_br00.node.node_profile.Color = 'blue'
            loc_br10 = db_model.Location.get(Name='br10')  # @UndefinedVariable
            self.assertEqual(loc_br10.node.node_profile.Color, 'blue')

    def test05_delete_traverse_cascade(self):
        db_model = self.db
        with db_session:
            node_prof_custom = db_model.Node_profile.get(Name='Custom')  # @UndefinedVariable
            node_id = list(node_prof_custom.nodes)[0].id
            node_prof_custom.delete()

        with db_session:
            node_prof_custom = db_model.Node_profile.get(Name='Custom')  # @UndefinedVariable
            self.assertIsNone(node_prof_custom)

            loc_br01 = db_model.Location.get(Name='br01')  # @UndefinedVariable
            node_br01 = loc_br01.node
            self.assertIsNone(node_br01)
            node_br01 = db_model.Node.get(id=node_id)  # @UndefinedVariable
            self.assertIsNone(node_br01)

        with db_session:
            loc_root = db_model.Location.get(Name='root')  # @UndefinedVariable
            loc_root.delete()

        with db_session:
            nodes = select(node for node in db_model.Node)[:]
            self.assertEqual(nodes, [])

            locations = select(location for location in db_model.Location)[:]
            self.assertEqual(locations, [])

            node_settings = select(node_setting for node_setting in db_model.Node_settings)[:]
            self.assertEqual(node_settings, [])

            node_logs = select(node_log for node_log in db_model.Node_log)[:]
            self.assertEqual(node_logs, [])