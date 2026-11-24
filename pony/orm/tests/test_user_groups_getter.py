import unittest

from pony.orm.core import user_groups_getter, usergroup_functions


class TestUserGroupsGetter(unittest.TestCase):
    def setUp(self):
        self.saved_functions = list(usergroup_functions)

    def tearDown(self):
        usergroup_functions[:] = self.saved_functions

    def test_decorator_returns_function(self):
        def get_groups(user):
            return "group"

        self.assertIs(user_groups_getter()(get_groups), get_groups)

    def test_function_registered_once(self):
        def get_groups(user):
            return "group"

        user_groups_getter()(get_groups)
        user_groups_getter()(get_groups)
        self.assertEqual(usergroup_functions.count((None, get_groups)), 1)

    def test_function_registered_for_different_classes(self):
        class A:
            pass

        class B:
            pass

        def get_groups(user):
            return "group"

        user_groups_getter(A)(get_groups)
        user_groups_getter(B)(get_groups)
        user_groups_getter(A)(get_groups)
        self.assertEqual(usergroup_functions.count((A, get_groups)), 1)
        self.assertEqual(usergroup_functions.count((B, get_groups)), 1)


if __name__ == "__main__":
    unittest.main()
