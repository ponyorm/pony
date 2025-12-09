from __future__ import absolute_import, print_function, division

import unittest

from pony.orm.core import *
from pony.orm.tests.testutils import *
from pony.orm.tests import setup_database, teardown_database

db = Database()

class Group(db.Entity):
    id = PrimaryKey(int)
    students = Set('Student')

class Student(db.Entity):
    name = Required(unicode)
    age = Required(int)
    group = Required('Group')
    scholarship = Required(int, default=0)
    courses = Set('Course')

class Course(db.Entity):
    name = Required(unicode)
    semester = Required(int)
    PrimaryKey(name, semester)
    students = Set('Student')


class TestQuerySetMonad(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_database(db)
        with db_session:
            g1 = Group(id=1)
            g2 = Group(id=2)
            s1 = Student(id=1, name='S1', age=20, group=g1, scholarship=0)
            s2 = Student(id=2, name='S2', age=23, group=g1, scholarship=100)
            s3 = Student(id=3, name='S3', age=23, group=g2, scholarship=500)
            c1 = Course(name='C1', semester=1, students=[s1, s2])
            c2 = Course(name='C2', semester=1, students=[s2, s3])
            c3 = Course(name='C3', semester=2, students=[s3])

    @classmethod
    def tearDownClass(cls):
        teardown_database(db)

    def setUp(self):
        rollback()
        db_session.__enter__()

    def tearDown(self):
        rollback()
        db_session.__exit__()

    def test_len(self):
        result = set(select(g for g in Group if len(g.students) > 1))
        self.assertEqual(result, {Group[1]})

    def test_len_2(self):
        result = set(select(g for g in Group if len(s for s in Student if s.group == g) > 1))
        self.assertEqual(result, {Group[1]})

    def test_len_3(self):
        result = set(select(g for g in Group if len(s.name for s in Student if s.group == g) > 1))
        self.assertEqual(result, {Group[1]})

    def test_count_1(self):
        result = set(select(g for g in Group if count(s.name for s in g.students) > 1))
        self.assertEqual(result, {Group[1]})

    def test_count_2(self):
        result = set(select(g for g in Group if select(s.name for s in g.students).count() > 1))
        self.assertEqual(result, {Group[1]})

    def test_count_3(self):
        result = set(select(s for s in Student if count(c for c in s.courses) > 1))
        self.assertEqual(result, {Student[2], Student[3]})

    def test_count_3a(self):
        result = set(select(s for s in Student if select(c for c in s.courses).count() > 1))
        self.assertEqual(result, {Student[2], Student[3]})
        self.assertTrue('DISTINCT' in db.last_sql)

    def test_count_3b(self):
        result = set(select(s for s in Student if select(c for c in s.courses).count(distinct=False) > 1))
        self.assertEqual(result, {Student[2], Student[3]})
        self.assertTrue('DISTINCT' not in db.last_sql)

    def test_count_4(self):
        result = set(select(c for c in Course if count(s for s in c.students) > 1))
        self.assertEqual(result, {Course['C1', 1], Course['C2', 1]})

    def test_count_5(self):
        result = select(c.semester for c in Course).count(distinct=True)
        self.assertEqual(result, 2)

    def test_count_6(self):
        result = select(c for c in Course).count()
        self.assertEqual(result, 3)
        self.assertTrue('DISTINCT' not in db.last_sql)

    def test_count_7(self):
        result = select(c for c in Course).count(distinct=True)
        self.assertEqual(result, 3)
        self.assertTrue('DISTINCT' in db.last_sql)

    def test_count_8(self):
        select(count(c.semester, distinct=False) for c in Course)[:]
        self.assertTrue('DISTINCT' not in db.last_sql)

    @raises_exception(TypeError, "`distinct` value should be True or False. Got: s.name.startswith('P')")
    def test_count_9(self):
        select(count(s, distinct=s.name.startswith('P')) for s in Student)

    def test_count_10(self):
        select(count('*', distinct=True) for s in Student)[:]
        self.assertTrue('DISTINCT' not in db.last_sql)

    @raises_exception(TypeError)
    def test_sum_1(self):
        result = set(select(g for g in Group if sum(s for s in Student if s.group == g) > 1))

    @raises_exception(TypeError)
    def test_sum_2(self):
        select(g for g in Group if sum(s.name for s in Student if s.group == g) > 1)

    def test_sum_3(self):
        result = sum(s.scholarship for s in Student)
        self.assertEqual(result, 600)

    def test_sum_4(self):
        result = sum(s.scholarship for s in Student if s.name == 'Unnamed')
        self.assertEqual(result, 0)

    def test_sum_5(self):
        result = select(c.semester for c in Course).sum()
        self.assertEqual(result, 4)

    def test_sum_6(self):
        result = select(c.semester for c in Course).sum(distinct=True)
        self.assertEqual(result, 3)

    def test_sum_7(self):
        result = set(select(g for g in Group if sum(s.scholarship for s in Student if s.group == g) > 500))
        self.assertEqual(result, set())

    def test_sum_8(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).sum() > 200))
        self.assertEqual(result, {Group[2]})
        self.assertTrue('DISTINCT' not in db.last_sql)

    def test_sum_9(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).sum(distinct=True) > 200))
        self.assertEqual(result, {Group[2]})
        self.assertTrue('DISTINCT' in db.last_sql)

    def test_sum_10(self):
        select(sum(s.scholarship, distinct=True) for s in Student)[:]
        self.assertTrue('SUM(DISTINCT' in db.last_sql)

    def test_min_1(self):
        result = set(select(g for g in Group if min(s.name for s in Student if s.group == g) == 'S1'))
        self.assertEqual(result, {Group[1]})

    @raises_exception(TypeError)
    def test_min_2(self):
        select(g for g in Group if min(s for s in Student if s.group == g) == None)

    def test_min_3(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).min() == 0))
        self.assertEqual(result, {Group[1]})

    def test_min_4(self):
        result = select(s.scholarship for s in Student).min()
        self.assertEqual(0, result)

    def test_max_1(self):
        result = set(select(g for g in Group if max(s.scholarship for s in Student if s.group == g) > 100))
        self.assertEqual(result, {Group[2]})

    @raises_exception(TypeError)
    def test_max_2(self):
        select(g for g in Group if max(s for s in Student if s.group == g) == None)

    def test_max_3(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).max() == 100))
        self.assertEqual(result, {Group[1]})

    def test_max_4(self):
        result = select(s.scholarship for s in Student).max()
        self.assertEqual(result, 500)

    def test_avg_1(self):
        result = select(g for g in Group if avg(s.scholarship for s in Student if s.group == g) == 50)[:]
        self.assertEqual(result, [Group[1]])

    def test_avg_2(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).avg() == 50))
        self.assertEqual(result, {Group[1]})

    def test_avg_3(self):
        result = select(c.semester for c in Course).avg()
        self.assertAlmostEqual(1.33, result, places=2)

    def test_avg_4(self):
        result = select(c.semester for c in Course).avg(distinct=True)
        self.assertAlmostEqual(1.5, result)

    def test_avg_5(self):
        result = set(select(g for g in Group if select(s.scholarship for s in g.students).avg(distinct=True) == 50))
        self.assertEqual(result, {Group[1]})
        self.assertTrue('AVG(DISTINCT' in db.last_sql)

    def test_avg_6(self):
        select(avg(s.scholarship, distinct=True) for s in Student)[:]
        self.assertTrue('AVG(DISTINCT' in db.last_sql)

    def test_exists_1(self):
        result = set(select(g for g in Group if exists(s for s in g.students if s.age < 23)))
        self.assertEqual(result, {Group[1]})

    def test_exists_2(self):
        result = set(select(g for g in Group if exists(s.age < 23 for s in g.students)))
        self.assertEqual(result, {Group[1]})

    def test_exists_3(self):
        result = set(select(g for g in Group if (s.age < 23 for s in g.students)))
        self.assertEqual(result, {Group[1]})

    def test_negate(self):
        result = set(select(g for g in Group if not(s.scholarship for s in Student if s.group == g)))
        self.assertEqual(result, set())

    def test_no_conditions(self):
        students = set(select(s for s in Student if s.group in (g for g in Group)))
        self.assertEqual(students, {Student[1], Student[2], Student[3]})

    def test_no_conditions_2(self):
        students = set(select(s for s in Student if s.scholarship == max(s.scholarship for s in Student)))
        self.assertEqual(students, {Student[3]})

    def test_hint_join_1(self):
        result = set(select(s for s in Student if JOIN(s.group in select(g for g in Group if g.id < 2))))
        self.assertEqual(result, {Student[1], Student[2]})

    def test_hint_join_2(self):
        result = set(select(s for s in Student if JOIN(s.group not in select(g for g in Group if g.id < 2))))
        self.assertEqual(result, {Student[3]})

    def test_hint_join_3(self):
        result = set(select(s for s in Student if JOIN(s.scholarship in
                        select(s.scholarship + 100 for s in Student if s.name != 'S2'))))
        self.assertEqual(result, {Student[2]})

    def test_hint_join_4(self):
        result = set(select(g for g in Group if JOIN(g in select(s.group for s in g.students))))
        self.assertEqual(result, {Group[1], Group[2]})

    def test_group_concat_1(self):
        result = select(s.name for s in Student).group_concat()
        self.assertEqual(result, 'S1,S2,S3')

    def test_group_concat_2(self):
        result = select(s.name for s in Student).group_concat('-')
        self.assertEqual(result, 'S1-S2-S3')

    def test_group_concat_3(self):
        result = select(s for s in Student if s.name in group_concat(s.name for s in Student))[:]
        self.assertEqual(set(result), {Student[1], Student[2], Student[3]})

    def test_group_concat_4(self):
        result = Student.select().group_concat()
        self.assertEqual(result, '1,2,3')

    def test_group_concat_5(self):
        result = Student.select().group_concat('.')
        self.assertEqual(result, '1.2.3')

    @raises_exception(TypeError, '`group_concat` cannot be used with entity with composite primary key')
    def test_group_concat_6(self):
        select(group_concat(s.courses, '-') for s in Student)

    def test_group_concat_7(self):
        result = select(group_concat(c.semester) for c in Course)[:]
        self.assertEqual(result[0], '1,1,2')

    def test_group_concat_8(self):
        result = select(group_concat(c.semester, '-') for c in Course)[:]
        self.assertEqual(result[0], '1-1-2')

    def test_group_concat_9(self):
        result = select(group_concat(c.semester, distinct=True) for c in Course)[:]
        self.assertEqual(result[0], '1,2')

    def test_group_concat_10(self):
        result = group_concat((s.name for s in Student if int(s.name[1]) > 1), sep='-')
        self.assertEqual(result, 'S2-S3')

    def test_group_concat_11(self):
        result = group_concat((c.semester for c in Course), distinct=True)
        self.assertEqual(result, '1,2')


    @raises_exception(TypeError, 'Query can only iterate over entity or another query (not a list of objects)')
    def test_select_from_select_1(self):
        query = select(s for s in Student if s.scholarship > 0)[:]
        result = set(select(x for x in query))
        self.assertEqual(result, {})

    def test_select_from_select_2(self):
        p, q = 50, 400
        query = select(s for s in Student if s.scholarship > p)
        result = select(x.id for x in query if x.scholarship < q)[:]
        self.assertEqual(set(result), {2})

    def test_select_from_select_3(self):
        p, q = 50, 400
        g = (s for s in Student if s.scholarship > p)
        result = select(x.id for x in g if x.scholarship < q)[:]
        self.assertEqual(set(result), {2})

    def test_select_from_select_4(self):
        p, q = 50, 400
        result = select(x.id for x in (s for s in Student if s.scholarship > p)
                             if x.scholarship < q)[:]
        self.assertEqual(set(result), {2})

    def test_select_from_select_5(self):
        p, q = 50, 400
        result = select(x.id for x in select(s for s in Student if s.scholarship > 0)
                             if x.scholarship < 400)[:]
        self.assertEqual(set(result), {2})

    def test_select_from_select_6(self):
        query = select(s.name for s in Student if s.scholarship > 0)
        result = select(x for x in query if not x.endswith('3'))
        self.assertEqual(set(result), {'S2'})

    @raises_exception(TranslationError, 'Too many values to unpack "for a, b in select(s for ...)" (expected 2, got 1)')
    def test_select_from_select_7(self):
        query = select(s for s in Student if s.scholarship > 0)
        result = select(a for a, b in query)

    @raises_exception(NotImplementedError, 'Please unpack a tuple of (s.name, s.group) in for-loop '
                                           'to individual variables (like: "for x, y in ...")')
    def test_select_from_select_8(self):
        query = select((s.name, s.group) for s in Student if s.scholarship > 0)
        result = select(x for x in query)

    @raises_exception(TranslationError, 'Not enough values to unpack "for x, y in '
                                        'select(s.name, s.group, s.scholarship for ...)" (expected 2, got 3)')
    def test_select_from_select_9(self):
        query = select((s.name, s.group, s.scholarship) for s in Student if s.scholarship > 0)
        result = select(x for x, y in query)

    def test_select_from_select_10(self):
        query = select((s.name, s.age) for s in Student if s.scholarship > 0)
        result = select(n for n, a in query if n.endswith('2') and a > 20)
        self.assertEqual(set(x for x in result), {'S2'})

    def test_aggregations_1(self):
        query = select((min(s.age), max(s.scholarship)) for s in Student)
        result = query[:]
        self.assertEqual(result, [(20, 500)])

    def test_aggregations_2(self):
        query = select((min(s.age), max(s.scholarship)) for s in Student for g in Group)
        result = query[:]
        self.assertEqual(result, [(20, 500)])


if __name__ == "__main__":
    unittest.main()
