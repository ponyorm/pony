import unittest
from datetime import timedelta

from pony import converting

class Test(unittest.TestCase):
    def test_timestamps_1(self):
        s = '1:2:3'
        td = converting.str2timedelta(s)
        self.assertEqual(td, timedelta(hours=1, minutes=2, seconds=3))
        s = '01:02:03'
        td = converting.str2timedelta(s)
        self.assertEqual(td, timedelta(hours=1, minutes=2, seconds=3))
        s = '1:2:3.456'
        td = converting.str2timedelta(s)
        self.assertEqual(td, timedelta(hours=1, minutes=2, seconds=3, milliseconds=456))
        s = '1:2:3.45678'
        td = converting.str2timedelta(s)
        self.assertEqual(td, timedelta(hours=1, minutes=2, seconds=3, microseconds=456780))
        s = '12:34:56'
        td = converting.str2timedelta(s)
        self.assertEqual(td, timedelta(hours=12, minutes=34, seconds=56))
        s = '12:34:56.789'
        td = converting.str2timedelta(s)
        self.assertEqual(td, timedelta(hours=12, minutes=34, seconds=56, milliseconds=789))
        s = '12:34:56.789123'
        td = converting.str2timedelta(s)
        self.assertEqual(td, timedelta(hours=12, minutes=34, seconds=56, microseconds=789123))

    def test_timestamps_2(self):
        s = '-1:2:3'
        td = converting.str2timedelta(s)
        self.assertEqual(td, -timedelta(hours=1, minutes=2, seconds=3))
        s = '-01:02:03'
        td = converting.str2timedelta(s)
        self.assertEqual(td, -timedelta(hours=1, minutes=2, seconds=3))
        s = '-1:2:3.456'
        td = converting.str2timedelta(s)
        self.assertEqual(td, -timedelta(hours=1, minutes=2, seconds=3, milliseconds=456))
        s = '-1:2:3.45678'
        td = converting.str2timedelta(s)
        self.assertEqual(td, -timedelta(hours=1, minutes=2, seconds=3, microseconds=456780))
        s = '-12:34:56'
        td = converting.str2timedelta(s)
        self.assertEqual(td, -timedelta(hours=12, minutes=34, seconds=56))
        s = '-12:34:56.789'
        td = converting.str2timedelta(s)
        self.assertEqual(td, -timedelta(hours=12, minutes=34, seconds=56, milliseconds=789))
        s = '-12:34:56.789123'
        td = converting.str2timedelta(s)
        self.assertEqual(td, -timedelta(hours=12, minutes=34, seconds=56, microseconds=789123))

    def test_timestamps_3(self):
        s = '0:1:2'
        td = converting.str2timedelta(s)
        self.assertEqual(td, timedelta(minutes=1, seconds=2))
        s = '0:1:2.3456'
        td = converting.str2timedelta(s)
        self.assertEqual(td, timedelta(minutes=1, seconds=2, microseconds=345600))

    def test_timestamps_4(self):
        s = '-0:1:2'
        td = converting.str2timedelta(s)
        self.assertEqual(td, -timedelta(minutes=1, seconds=2))
        s = '-0:1:2.3456'
        td = converting.str2timedelta(s)
        self.assertEqual(td, -timedelta(minutes=1, seconds=2, microseconds=345600))
