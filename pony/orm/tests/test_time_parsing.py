from __future__ import absolute_import, print_function, division

import unittest
from datetime import datetime, date, time

from pony.orm.tests.testutils import raises_exception
from pony.converting import str2time


class TestTimeParsing(unittest.TestCase):
    def test_time_1(self):
        self.assertEqual(str2time('1:2'), time(1, 2))
        self.assertEqual(str2time('01:02'), time(1, 2))
        self.assertEqual(str2time('1:2:3'), time(1, 2, 3))
        self.assertEqual(str2time('01:02:03'), time(1, 2, 3))
        self.assertEqual(str2time('1:2:3.4'), time(1, 2, 3, 400000))
        self.assertEqual(str2time('01:02:03.4'), time(1, 2, 3, 400000))

    @raises_exception(ValueError, 'Unrecognized time format')
    def test_time_2(self):
        str2time('1:')

    @raises_exception(ValueError, 'Unrecognized time format')
    def test_time_3(self):
        str2time('1: 2')

    @raises_exception(ValueError, 'Unrecognized time format')
    def test_time_4(self):
        str2time('1:2:')

    @raises_exception(ValueError, 'Unrecognized time format')
    def test_time_5(self):
        str2time('1:2:3:')

    @raises_exception(ValueError, 'Unrecognized time format')
    def test_time_6(self):
        str2time('1:2:3.1234567')

    def test_time_7(self):
        self.assertEqual(str2time('1:33 am'), time(1, 33))
        self.assertEqual(str2time('2:33 am'), time(2, 33))
        self.assertEqual(str2time('11:33 am'), time(11, 33))
        self.assertEqual(str2time('12:33 am'), time(0, 33))

    def test_time_8(self):
        self.assertEqual(str2time('1:33 pm'), time(13, 33))
        self.assertEqual(str2time('2:33 pm'), time(14, 33))
        self.assertEqual(str2time('11:33 pm'), time(23, 33))
        self.assertEqual(str2time('12:33 pm'), time(12, 33))

    def test_time_9(self):
        self.assertEqual(str2time('1:33am'), time(1, 33))
        self.assertEqual(str2time('1:33 am'), time(1, 33))
        self.assertEqual(str2time('1:33 AM'), time(1, 33))
        self.assertEqual(str2time('1:33 a.m'), time(1, 33))
        self.assertEqual(str2time('1:33 A.M'), time(1, 33))
        self.assertEqual(str2time('1:33 a.m.'), time(1, 33))
        self.assertEqual(str2time('1:33 A.M.'), time(1, 33))

    def test_time_10(self):
        self.assertEqual(str2time('1:33pm'), time(13, 33))
        self.assertEqual(str2time('1:33 pm'), time(13, 33))
        self.assertEqual(str2time('1:33 PM'), time(13, 33))
        self.assertEqual(str2time('1:33 p.m'), time(13, 33))
        self.assertEqual(str2time('1:33 P.M'), time(13, 33))
        self.assertEqual(str2time('1:33 p.m.'), time(13, 33))
        self.assertEqual(str2time('1:33 P.M.'), time(13, 33))

    def test_time_11(self):
        self.assertEqual(str2time('12:34:56.789'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12.34.56.789'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12 34 56.789'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12h34m56.789'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12h 34m 56.789'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12 h 34 m 56.789'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12h 34m 56.789s'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12 h 34 m 56.789 s'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12h 34min 56.789'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12h 34min 56.789sec'), time(12, 34, 56, 789000))
        self.assertEqual(str2time('12h 34 min 56.789 sec'), time(12, 34, 56, 789000))

    def test_time_12(self):
        self.assertEqual(str2time('12:34:56.789 a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12.34.56.789 a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12 34 56.789 a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12h34m56.789 a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12h 34m 56.789 a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12 h 34 m 56.789 a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12h 34m 56.789s a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12 h 34 m 56.789 s a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12h 34min 56.789 a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12h 34min 56.789sec a.m.'), time(0, 34, 56, 789000))
        self.assertEqual(str2time('12h 34 min 56.789 sec a.m.'), time(0, 34, 56, 789000))

    def test_time_13(self):
        self.assertEqual(str2time('12:34'), time(12, 34))
        self.assertEqual(str2time('12.34'), time(12, 34))
        self.assertEqual(str2time('12 34'), time(12, 34))
        self.assertEqual(str2time('12h34'), time(12, 34))
        self.assertEqual(str2time('12h34m'), time(12, 34))
        self.assertEqual(str2time('12h 34m'), time(12, 34))
        self.assertEqual(str2time('12h34min'), time(12, 34))
        self.assertEqual(str2time('12h 34min'), time(12, 34))
        self.assertEqual(str2time('12 h 34 m'), time(12, 34))
        self.assertEqual(str2time('12 h 34 min'), time(12, 34))
        self.assertEqual(str2time('12u34'), time(12, 34))  # Belgium
        self.assertEqual(str2time('12h'), time(12))
        self.assertEqual(str2time('12u'), time(12))

    def test_time_14(self):
        self.assertEqual(str2time('12:34 am'), time(0, 34))
        self.assertEqual(str2time('12.34 am'), time(0, 34))
        self.assertEqual(str2time('12 34 am'), time(0, 34))
        self.assertEqual(str2time('12h34 am'), time(0, 34))
        self.assertEqual(str2time('12h34m am'), time(0, 34))
        self.assertEqual(str2time('12h 34m am'), time(0, 34))
        self.assertEqual(str2time('12h34min am'), time(0, 34))
        self.assertEqual(str2time('12h 34min am'), time(0, 34))
        self.assertEqual(str2time('12 h 34 m am'), time(0, 34))
        self.assertEqual(str2time('12 h 34 min am'), time(0, 34))
        self.assertEqual(str2time('12u34 am'), time(0, 34))
        self.assertEqual(str2time('12h am'), time(0))
        self.assertEqual(str2time('12u am'), time(0))


if __name__ == '__main__':
    unittest.main()
