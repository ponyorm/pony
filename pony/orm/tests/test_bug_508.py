"""Regression tests for issue #508.

Navigating an Optional relationship inside an OR expression must not
drop rows where the relationship is NULL.
"""

import unittest

from pony.orm import *
from pony.orm.tests import setup_database, teardown_database

db = Database()


class User(db.Entity):
    id = PrimaryKey(str)
    given_name = Required(str)
    surname = Required(str)


class Agent(User):
    service_desk = Optional("ServiceDesk")


class ServiceDesk(db.Entity):
    id = PrimaryKey(str)
    name = Required(str, unique=True)
    agents = Set(Agent)
    department = Required("Department")


class Department(db.Entity):
    id = PrimaryKey(str)
    name = Required(str)
    desks = Set(ServiceDesk)


class TestOrWithOptionalAttr(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            sales = Department(id="d1", name="Sales")
            support = Department(id="d2", name="Marta support")
            desk = ServiceDesk(id="sd1", name="Desk", department=sales)
            marta_desk = ServiceDesk(id="sd2", name="Marta desk", department=sales)
            other_desk = ServiceDesk(id="sd3", name="Other", department=support)
            Agent(id="a1", given_name="Marta", surname="X")
            Agent(id="a2", given_name="Bob", surname="Y", service_desk=desk)
            Agent(id="a3", given_name="Carl", surname="Z", service_desk=marta_desk)
            Agent(id="a4", given_name="Dan", surname="W", service_desk=other_desk)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def ids(self, query):
        return sorted(obj.id for obj in query)

    def test_filter_or_with_null_relationship(self):
        query = Agent.select().filter(
            lambda e: "Marta" in e.given_name
            or "Marta" in e.surname
            or "Marta" in e.service_desk.name
        )
        self.assertEqual(self.ids(query), ["a1", "a3"])
        self.assertIn("LEFT JOIN", db.last_sql)

    def test_select_or_with_null_relationship(self):
        query = select(
            e
            for e in Agent
            if e.given_name == "Marta" or e.service_desk.name == "Marta desk"
        )
        self.assertEqual(self.ids(query), ["a1", "a3"])

    def test_or_nested_inside_and(self):
        query = select(
            e
            for e in Agent
            if e.surname != "Y"
            and (e.given_name == "Marta" or e.service_desk.name == "Marta desk")
        )
        self.assertEqual(self.ids(query), ["a1", "a3"])

    def test_or_with_chained_relationship(self):
        query = select(
            e
            for e in Agent
            if e.given_name == "Marta" or "Marta" in e.service_desk.department.name
        )
        self.assertEqual(self.ids(query), ["a1", "a4"])

    def test_or_mixed_with_and_condition_on_same_relationship(self):
        query = select(
            e
            for e in Agent
            if e.service_desk.name != "Desk"
            and (e.given_name == "Marta" or e.service_desk.name == "Marta desk")
        )
        # a1 has no service desk, so the AND condition excludes it
        self.assertEqual(self.ids(query), ["a3"])

    def test_without_or_inner_join_is_kept(self):
        query = select(e for e in Agent if "Marta" in e.service_desk.name)
        self.assertEqual(self.ids(query), ["a3"])
        self.assertNotIn("LEFT JOIN", db.last_sql)

    def test_required_relationship_in_or_keeps_inner_join(self):
        query = select(
            d for d in ServiceDesk if d.name == "Desk" or d.department.name == "Marta support"
        )
        self.assertEqual(self.ids(query), ["sd1", "sd3"])
        self.assertNotIn("LEFT JOIN", db.last_sql)

    def test_or_with_null_check_does_not_join(self):
        query = select(
            e for e in Agent if e.given_name == "Bob" or e.service_desk is None
        )
        self.assertEqual(self.ids(query), ["a1", "a2"])
        self.assertNotIn("JOIN", db.last_sql)

    def test_join_hint_with_or_keeps_filter(self):
        query = select(
            e
            for e in Agent
            if JOIN(e.id in select(a.id for a in Agent if a.given_name == "Bob"))
            and (e.given_name == "Marta" or e.service_desk.name == "Marta desk")
        )
        self.assertEqual(self.ids(query), [])

    def test_or_inside_subquery(self):
        query = select(
            e
            for e in Agent
            if exists(
                a
                for a in Agent
                if a.id == e.id
                and (a.given_name == "Marta" or a.service_desk.name == "Marta desk")
            )
        )
        self.assertEqual(self.ids(query), ["a1", "a3"])


if __name__ == "__main__":
    unittest.main()
