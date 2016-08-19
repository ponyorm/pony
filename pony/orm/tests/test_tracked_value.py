import unittest

from pony.orm.ormtypes import TrackedList, TrackedDict, TrackedValue

class Object(object):
    def __init__(self):
        self.on_attr_changed = None
    def _attr_changed_(self, attr):
        if self.on_attr_changed is not None:
            self.on_attr_changed(attr)


class Attr(object):
    pass


class TestTrackedValue(unittest.TestCase):

    def test_make(self):
        obj = Object()
        attr = Attr()
        value = {'items': ['one', 'two', 'three']}
        tracked_value = TrackedValue.make(obj, attr, value)
        self.assertEqual(type(tracked_value), TrackedDict)
        self.assertEqual(type(tracked_value['items']), TrackedList)

    def test_dict_setitem(self):
        obj = Object()
        attr = Attr()
        value = {'items': ['one', 'two', 'three']}
        tracked_value = TrackedValue.make(obj, attr, value)
        log = []
        obj.on_attr_changed = lambda x: log.append(x)
        tracked_value['items'] = [1, 2, 3]
        self.assertEqual(log, [attr])

    def test_list_append(self):
        obj = Object()
        attr = Attr()
        value = {'items': ['one', 'two', 'three']}
        tracked_value = TrackedValue.make(obj, attr, value)
        log = []
        obj.on_attr_changed = lambda x: log.append(x)
        tracked_value['items'].append('four')
        self.assertEqual(log, [attr])

    def test_list_setslice(self):
        obj = Object()
        attr = Attr()
        value = {'items': ['one', 'two', 'three']}
        tracked_value = TrackedValue.make(obj, attr, value)
        log = []
        obj.on_attr_changed = lambda x: log.append(x)
        tracked_value['items'][1:2] = ['a', 'b', 'c']
        self.assertEqual(log, [attr])
        self.assertEqual(tracked_value['items'], ['one', 'a', 'b', 'c', 'three'])
