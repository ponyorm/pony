import ast
import unittest
from typing import TYPE_CHECKING, cast

from pony import orm
from pony.orm.sqltranslation import SQLTranslator
from pony.orm.tests import setup_database, teardown_database
from pony.orm.tests.testutils import raises_exception

db = orm.Database()

if TYPE_CHECKING:
    from pony.orm.core import Entity
else:
    Entity = db.Entity


class Person(Entity):
    name = orm.Required(str)


class TestTranslatorUnsupported(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    @raises_exception(
        orm.TranslationError,
        "Delete query should be applied to a single entity. Got: p.name",
    )
    @orm.db_session
    def test_bulk_delete_of_non_entity(self):
        orm.select(p.name for p in Person).delete(bulk=True)

    @raises_exception(NotImplementedError, "~p.id")
    @orm.db_session
    def test_invert_operator(self):
        orm.select(p for p in Person if ~p.id == -2)

    @raises_exception(NotImplementedError, "**kwargs is not supported")
    def test_double_star_call_argument(self):
        node = ast.parse("f(**kwargs)", mode="eval").body
        assert isinstance(node, ast.Call)
        SQLTranslator.preCall(cast("SQLTranslator", None), node)


if __name__ == "__main__":
    unittest.main()
