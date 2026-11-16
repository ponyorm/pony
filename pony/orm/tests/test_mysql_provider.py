import unittest
from datetime import timedelta
from importlib.util import find_spec
from unittest import mock

HAS_MYSQL_DRIVER = any(find_spec(name) for name in ("MySQLdb", "pymysql"))


@unittest.skipUnless(HAS_MYSQL_DRIVER, "no MySQL driver is installed")
class TestMySQLConverters(unittest.TestCase):
    def test_encode_timedelta(self):
        from pony.orm.dbproviders import mysql as mysql_provider

        # MySQLdb's string_literal is a C function that takes exactly one
        # argument, so the converter must not pass anything else to it
        def string_literal(value):
            return "'%s'" % value

        with mock.patch.object(mysql_provider, "string_literal", string_literal):
            provider = object.__new__(mysql_provider.MySQLProvider)
            conv = provider.get_pool().kwargs["conv"]
            encoded = conv[timedelta](timedelta(hours=1, minutes=2, seconds=3))
        self.assertEqual(encoded, "'1:2:3'")


if __name__ == "__main__":
    unittest.main()
