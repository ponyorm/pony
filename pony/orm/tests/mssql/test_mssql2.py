'''
Probably, excessive tests
Probably, will be removed
'''

from .util import *

from .test_mssql import MSSQLSetup, TestCase, PY2

import unittest
from datetime import datetime, date

from pony.orm import *

class Test(MSSQLSetup.as_mixin(), TestCase):

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
            Start_date = Required(date)
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
        super(Test, cls).setUpClass()
        with db_session:
            db_model_2 = cls.db
            #create root location and 2 branches, each with 2 more children - 7 locations in total
            loc_root = db_model_2.Location(Name='root')
            loc_br0 = db_model_2.Location(Name='br0', parent=loc_root)
            loc_br1 = db_model_2.Location(Name='br1', parent=loc_root)
            loc_br00 = db_model_2.Location(Name='br00', parent=loc_br0)
            loc_br01 = db_model_2.Location(Name='br01', parent=loc_br0)
            loc_br10 = db_model_2.Location(Name='br10', parent=loc_br1)
            loc_br11 = db_model_2.Location(Name='br11', parent=loc_br1)

            #create 2 node profiles
            node_prof_default = db_model_2.Node_profile(Name='Default', Color='white', HTML_page=html_page_default)
            node_prof_custom = db_model_2.Node_profile(Name='Custom', Color='white', HTML_page=html_page_custom)

            #create 3 nodes in br00, 01, 10
            node_br00 = db_model_2.Node(location=loc_br00, Enabled=True, Cores=1, Start_date=date(2015, 12, 1)
                            , node_profile=node_prof_default)
            node_br01 = db_model_2.Node(location=loc_br01, Enabled=True, Cores=4, Start_date=date(2016, 1, 1)
                            , node_profile=node_prof_custom)
            node_br10 = db_model_2.Node(location=loc_br10, Enabled=True, Cores=4, Start_date=date(2016, 2, 1)
                            , node_profile=node_prof_default)

            #create node settings for all
            node_set_00 = db_model_2.Node_settings(IP_Address='1.2.3.4', IP_Netmask='255.255.0.0'
                                        , HW_version='B01', Display_resolution='800x600'
                                        , Display_wallpaper=wallpaper, node=node_br00)
            node_set_01 = db_model_2.Node_settings(IP_Address='1.2.3.5', IP_Netmask='255.255.0.0'
                                        , HW_version='B02', Display_resolution='800x600'
                                        , Display_wallpaper=wallpaper, node=node_br01)
            node_set_10 = db_model_2.Node_settings(IP_Address='1.2.3.6', IP_Netmask='255.255.0.0'
                                        , HW_version='B03', Display_resolution='1024x768'
                                        , Display_wallpaper=wallpaper, node=node_br10)

            #create 10 log entries for the 3 nodes
            node_log_000 = db_model_2.Node_log(Timestamp=datetime(2015, 12, 1, 0, 0, 10), Location_name='br00'
                                , Event='Startup', Data='', node=node_br00)
            node_log_010 = db_model_2.Node_log(Timestamp=datetime(2016, 1, 1, 0, 0, 10), Location_name='br01'
                                , Event='Startup', Data='', node=node_br01)
            node_log_100 = db_model_2.Node_log(Timestamp=datetime(2016, 2, 1, 0, 0, 10), Location_name='br10'
                                , Event='Startup', Data='', node=node_br10)
            node_log_001 = db_model_2.Node_log(Timestamp=datetime(2015, 12, 1, 2, 0, 10), Location_name='br00'
                                , Event='Rain start', Data='', node=node_br00)
            node_log_002 = db_model_2.Node_log(Timestamp=datetime(2015, 12, 1, 2, 0, 20), Location_name='br00'
                                , Event='Temp measure', Data='23.5', node=node_br00)
            node_log_003 = db_model_2.Node_log(Timestamp=datetime(2015, 12, 1, 2, 5, 20), Location_name='br00'
                                , Event='Rain L/m2/h', Data='0.2', node=node_br00)
            node_log_004 = db_model_2.Node_log(Timestamp=datetime(2015, 12, 1, 2, 5, 40), Location_name='br00'
                                , Event='Temp measure', Data='23.0', node=node_br00)
            node_log_005 = db_model_2.Node_log(Timestamp=datetime(2015, 12, 1, 5, 34, 25), Location_name='br00'
                                , Event='Rain stop', Data='', node=node_br00)
            node_log_006 = db_model_2.Node_log(Timestamp=datetime(2015, 12, 1, 5, 40, 20), Location_name='br00'
                                , Event='Rain L/m2/h', Data='0.48', node=node_br00)
            node_log_007 = db_model_2.Node_log(Timestamp=datetime(2015, 12, 1, 5, 40, 40), Location_name='br00'
                                , Event='Temp measure', Data='21.5', node=node_br00)

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
        db_model_2 = self.db
        #create 1 more location, node_profile, node, node_setting, node_log
        with db_session:
            loc_br1 = db_model_2.Location.get(Name='br1')  # @UndefinedVariable
            loc_br12 = db_model_2.Location(Name='br12', parent=loc_br1)

            node_prof_custom2 = db_model_2.Node_profile(Name='Custom2', Color='green', HTML_page=html_page_custom)

            node_br12 = db_model_2.Node(location=loc_br12, Enabled=True, Cores=4, Start_date=date(2016, 5, 1)
                             , node_profile=node_prof_custom2)

            node_set_12 = db_model_2.Node_settings(IP_Address='1.2.3.7', IP_Netmask='255.255.0.0'
                                        , HW_version='B03', Display_resolution='1024x768'
                                        , Display_wallpaper=wallpaper, node=node_br12)

            node_log_120 = db_model_2.Node_log(Timestamp=datetime(2016, 5, 1, 0, 0, 10), Location_name='br12'
                                   , Event='Startup', Data='', node=node_br12)
            node_log_121 = db_model_2.Node_log(Timestamp=datetime(2016, 5, 1, 0, 0, 50), Location_name='br12'
                                   , Event='Temp measure', Data='23.0', node=node_br12)

        with db_session:
            loc_br1 = db_model_2.Location.get(Name='br1')  # @UndefinedVariable
            self.check_attrs(loc_br1)
            loc_br12 = db_model_2.Location.get(Name='br12')  # @UndefinedVariable
            self.check_attrs(loc_br12)
            self.assertIs(loc_br12.parent, loc_br1)

            node_prof_custom2 = db_model_2.Node_profile.get(Name='Custom2')  # @UndefinedVariable
            self.check_attrs(node_prof_custom2, {'Name':'Custom2', 'Color':'green', 'HTML_page':html_page_custom})

            node_br12 = loc_br12.node
            self.check_attrs(node_br12, {'Enabled':True, 'Cores':4, 'Start_date':date(2016, 5, 1)})
            self.assertIs(node_br12.location, loc_br12)
            self.assertIs(node_br12.node_profile, node_prof_custom2)

            node_logs = select(node_log for node_log in db_model_2.Node_log if node_log.node is node_br12)
            self.assertEqual(set([node_log.Event for node_log in node_logs]), set(('Startup', 'Temp measure')))
            node_logs = {node_log.Event:node_log for node_log in node_logs}

            self.check_attrs(node_logs['Startup'],{'Timestamp':datetime(2016, 5, 1, 0, 0, 10), 'Location_name':'br12'
                                   , 'Event':'Startup', 'Data':''})
            self.assertIs(node_logs['Startup'].node, node_br12)
            self.check_attrs(node_logs['Temp measure'],{'Timestamp':datetime(2016, 5, 1, 0, 0, 50), 'Location_name':'br12'
                                   , 'Event':'Temp measure', 'Data':'23.0'})
            self.assertIs(node_logs['Temp measure'].node, node_br12)

    def test03_read_traverse(self):
        db_model_2 = self.db
        #read locations and corresponding nodes, node_profiles, node_setings, node_logs
        with db_session:
            locations = select(location for location in db_model_2.Location)
            locations_tree = build_locations_tree(locations)
            self.assertEqual(set(locations_tree.keys()), set(('root', 'br0', 'br1', 'br00', 'br01', 'br10', 'br11', 'br12')))
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
            self.check_attrs(node_br00, {'Enabled':True, 'Cores':1, 'Start_date':date(2015, 12, 1)})
            self.check_attrs(node_br01, {'Enabled':True, 'Cores':4, 'Start_date':date(2016, 1, 1)})
            self.check_attrs(node_br10, {'Enabled':True, 'Cores':4, 'Start_date':date(2016, 2, 1)})

            node_prof_default = db_model_2.Node_profile.get(Name='Default')  # @UndefinedVariable
            self.assertIs(node_prof_default, node_br00.node_profile)
            self.assertIs(node_prof_default, node_br10.node_profile)
            node_prof_custom = db_model_2.Node_profile.get(Name='Custom')  # @UndefinedVariable
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
        db_model_2 = self.db
        with db_session:
            location_child = db_model_2.Location.get(Name='br1')  # @UndefinedVariable
            location_parent = db_model_2.Location.get(Name='br0')  # @UndefinedVariable
            location_child.parent = location_parent

        with db_session:
            locations = select(location for location in db_model_2.Location)
            locations_tree = build_locations_tree(locations)
            self.assertEqual(set(locations_tree.keys()), set(('root', 'br0', 'br1', 'br00', 'br01', 'br10', 'br11', 'br12')))
            self.assertIn(locations_tree['br0'].value, locations_tree['root'].value.children)
            self.assertIn(locations_tree['br1'].value, locations_tree['br0'].value.children)
            self.assertIn(locations_tree['br00'].value, locations_tree['br0'].value.children)
            self.assertIn(locations_tree['br01'].value, locations_tree['br0'].value.children)
            self.assertIn(locations_tree['br10'].value, locations_tree['br1'].value.children)
            self.assertIn(locations_tree['br11'].value, locations_tree['br1'].value.children)

        with db_session:
            loc_br00 = db_model_2.Location.get(Name='br00')  # @UndefinedVariable
            loc_br00.node.node_profile.Color = 'blue'
            loc_br10 = db_model_2.Location.get(Name='br10')  # @UndefinedVariable
            self.assertEqual(loc_br10.node.node_profile.Color, 'blue')

    def test05_delete_traverse_cascade(self):
        db_model_2 = self.db
        with db_session:
            node_prof_custom = db_model_2.Node_profile.get(Name='Custom')  # @UndefinedVariable
            node_id = list(node_prof_custom.nodes)[0].id
            node_prof_custom.delete()

        with db_session:
            node_prof_custom = db_model_2.Node_profile.get(Name='Custom')  # @UndefinedVariable
            self.assertIsNone(node_prof_custom)

            loc_br01 = db_model_2.Location.get(Name='br01')  # @UndefinedVariable
            node_br01 = loc_br01.node
            self.assertIsNone(node_br01)
            node_br01 = db_model_2.Node.get(id=node_id)  # @UndefinedVariable
            self.assertIsNone(node_br01)

        with db_session:
            loc_root = db_model_2.Location.get(Name='root')  # @UndefinedVariable
            loc_root.delete()

        with db_session:
            nodes = select(node for node in db_model_2.Node)[:]
            self.assertEqual(nodes, [])

            locations = select(location for location in db_model_2.Location)[:]
            self.assertEqual(locations, [])

            node_settings = select(node_setting for node_setting in db_model_2.Node_settings)[:]
            self.assertEqual(node_settings, [])

            node_logs = select(node_log for node_log in db_model_2.Node_log)[:]
            self.assertEqual(node_logs, [])