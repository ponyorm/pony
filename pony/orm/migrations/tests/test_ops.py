import unittest
import collections
from datetime import datetime, date, time
from decimal import Decimal
from pony.orm import *
from pony.orm.migrations import VirtualDB, Migration
from collections import defaultdict

"""
TODO: remove max_len,
      error on change primary key attr type  
      move column after change required -> optional and vice versa
"""


class TestMigrations(unittest.TestCase):
    db_params = dict(provider='postgres', user='ponytest', password='ponytest', host='localhost', database='ponytest')

    def setUp(self):
        self.db = Database(**self.db_params)
        db = self.db

        class Department(db.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        db.generate_mapping(check_tables=False)
        db.drop_all_tables(with_all_data=True)
        db.create_tables()

    def tearDown(self):
        if self.db2.schema:
            self.db2.drop_all_tables()

    def apply_migrate(self, rename_map=None):
        vdb = VirtualDB.from_db(self.db)
        vdb.schema = self.db.provider.vdbschema_cls.from_vdb(vdb, self.db.provider)
        vdb.vdb_only = False

        self.db2.generate_mapping(check_tables=False)
        from_vdb = self.db.vdb
        to_vdb = self.db2.vdb
        m = Migration.make(from_vdb, to_vdb, rename_map)

        lines = []
        for op in m.operations:
            imports = collections.defaultdict(set)
            lines.append(op.serialize(imports))
            op_text = ''
            for k, v in imports.items():
                op_text += 'from %s import %s\n' % (k, ', '.join(str(i) for i in v))
            op_text += '\n'
            op_text += 'op = ' + lines[-1]
            # print('***', op_text)
            objects = {}
            exec(op_text, objects)
            op = objects['op']
            op.apply(vdb)

        expected_schema = to_vdb.schema
        actual_schema = vdb.schema

        with db_session:
            connection = self.db.get_connection()
            sql_ops = actual_schema.apply(connection, False, False)

        return expected_schema, actual_schema, m, sql_ops

    def test_add_entity(self):
        """
            Adds regular entity "Course mark"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            course_mark = Optional('CourseMark')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')
            course_marks = Set('CourseMark')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        class CourseMark(db2.Entity):  # added entity
            id = PrimaryKey(int, auto=True)
            course = Required(Course)
            student = Optional(Student)
            mark = Required(int)

        migration_op = "AddEntity(Entity('CourseMark',  attrs=[\n        " \
                       "PrimaryKey('id', int, auto=True), \n        " \
                       "Required('course', 'Course', reverse=Optional('course_mark', 'CourseMark')), \n        " \
                       "Optional('student', 'Student', reverse=Set('course_marks', 'CourseMark')), \n        " \
                       "Required('mark', int)]))"

        correct_sql = 'CREATE TABLE "coursemark" (\n  ' \
                        '"id" SERIAL PRIMARY KEY,\n  ' \
                        '"course_name" TEXT NOT NULL,\n  ' \
                        '"course_semester" INTEGER NOT NULL,\n  ' \
                        '"student_id" INTEGER,\n  ' \
                        '"mark" INTEGER NOT NULL\n' \
                      ')\n' \
                      'CREATE INDEX "idx_coursemark__course_name__course_semester" ON ' \
                      '"coursemark" ("course_name", "course_semester")\n' \
                      'CREATE INDEX "idx_coursemark__student_id" ON "coursemark" ("student_id")\n' \
                      'ALTER TABLE "coursemark" ADD CONSTRAINT "fk_coursemark__course_name__course_semester" ' \
                        'FOREIGN KEY ("course_name", "course_semester") REFERENCES "course" ("name", "semester") ON DELETE CASCADE\n' \
                      'ALTER TABLE "coursemark" ADD CONSTRAINT "fk_coursemark__student_id" ' \
                        'FOREIGN KEY ("student_id") REFERENCES "student" ("id") ON DELETE SET NULL'


        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_inherited_entity(self):
        """
            Adds inherited entity "Graduate"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        class Graduate(Student):  # added inherited entity
            doc_number = Required(str)
            exam_mark = Required(int)

        migration_op = "AddEntity(Entity('Graduate', bases=['Student'], attrs=[\n        " \
                       "Required('doc_number', str), \n        " \
                       "Required('exam_mark', int)]))"

        correct_sql = 'ALTER TABLE "student" ADD COLUMN "classtype" TEXT DEFAULT \'Student\' NOT NULL\n' \
                      'ALTER TABLE "student" ALTER COLUMN "classtype" DROP DEFAULT\n' \
                      'ALTER TABLE "student" ADD COLUMN "doc_number" TEXT\n' \
                      'ALTER TABLE "student" ADD COLUMN "exam_mark" INTEGER'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_rename_entity(self):
        """
            Renames entity "Student" to "Pupil"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Pupil')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Pupil')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Pupil(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Pupil)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = '\n'.join([
            'ALTER TABLE "course_students" RENAME COLUMN "student_id" TO "pupil_id"',
            'ALTER TABLE "course_students" RENAME CONSTRAINT "fk_course_students__student_id" TO "fk_course_students__pupil_id"',
            'ALTER INDEX "idx_course_students__student_id" RENAME TO "idx_course_students__pupil_id"',
            'ALTER TABLE "student" RENAME TO "pupil"',
            'ALTER TABLE "pupil" RENAME CONSTRAINT "fk_student__group_number" TO "fk_pupil__group_number"',
            'ALTER TABLE "pupil" RENAME CONSTRAINT "fk_student__mentor_id" TO "fk_pupil__mentor_id"',
            'ALTER INDEX "idx_student__group_number" RENAME TO "idx_pupil__group_number"',
            'ALTER INDEX "idx_student__mentor_id" RENAME TO "idx_pupil__mentor_id"'
        ])

        migration_op = "RenameEntity(entity_name='Student', new_entity_name='Pupil')"

        rename_map = {'Student': 'Pupil'}
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate(rename_map)
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_entity(self):
        """
            Delete entity "Group"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "student" DROP COLUMN "group_number"\n' \
                      'DROP TABLE "group"'

        migration_op = "RemoveEntity('Group')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_inherited_entity(self):
        """
            Delete entity "DeptDirector"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            student = Optional(Student)

        migration_op = "RemoveEntity('DeptDirector')\n" \
                       "RemoveAttribute(entity_name='Teacher', attr_name='head_of_dept')"

        correct_sql = 'ALTER TABLE "teacher" DROP COLUMN "classtype"\n' \
                      'ALTER TABLE "teacher" DROP COLUMN "is_director"\n' \
                      'ALTER TABLE "teacher" DROP COLUMN "teacher_id"'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_any_table_name(self):
        """
            Set's table name "dept" to entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            _table_ = 'dept'
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "department" RENAME TO "dept"'

        migration_op = "RenameTable(entity_name='Department', new_table_name='dept')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_required_attr(self):
        """
            Add's required attribute "code" with initial value to entity "Course"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            code = Required(str, initial="00.00.00")
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "course" ADD COLUMN "code" TEXT DEFAULT \'00.00.00\' NOT NULL\n' \
                      'ALTER TABLE "course" ALTER COLUMN "code" DROP DEFAULT'

        migration_op = "AddAttribute(entity_name='Course', attr=Required('code', str, initial='00.00.00'))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_optional_attr(self):
        """
            Add's optional attribute "patronymic" to entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            patronymic = Optional(str)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "teacher" ADD COLUMN "patronymic" TEXT DEFAULT \'\' NOT NULL'

        migration_op = "AddAttribute(entity_name='Teacher', attr=Optional('patronymic', str))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_attr_name(self):
        """
            Change attribute name "dob" to "date_of_birth" in entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            date_of_birth = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "teacher" RENAME COLUMN "dob" TO "date_of_birth"'

        migration_op = "RenameAttribute(entity_name='Teacher', attr_name='dob', new_attr_name='date_of_birth')"

        rename_map = {('Teacher', 'dob'): ('Teacher', 'date_of_birth')}
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate(rename_map=rename_map)
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_max_length(self):
        """
            Change max_length parameter for string attribute. Attribute "name" in entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 300)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeColumnType(entity_name='Department', attr_name='name', py_type=str, options={'max_len': 300})"

        correct_sql = 'ALTER TABLE "department" ALTER COLUMN "name" TYPE VARCHAR(300)'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_int_size(self):
        """
            Change int attribute size. Attribute "credits" in entity "Course"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=32)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "credits" TYPE INTEGER'

        migration_op = "ChangeColumnType(entity_name='Course', attr_name='credits', py_type=int, options={'size': 32})"
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_unset_nullable_attr(self):
        """
            Unset's "nullable" to attribute "biography" in entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeNullable(entity_name='Teacher', attr_name='biography', nullable=False)"

        correct_sql = 'ALTER TABLE "teacher" ALTER COLUMN "biography" SET NOT NULL'

        # test execution freezes at apply_migrate() call
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_nullable_attr(self):
        """
            Set's "nullable" to attribute "description" in entity "Course"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str, nullable=True)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeNullable(entity_name='Course', attr_name='description', nullable=True)"

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "description" DROP NOT NULL'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_attr(self):
        """
            Delete's attribute: optional "gpa" from entity "Student" and required "lab_hours" from "Course"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "course" DROP COLUMN "lab_hours"\n' \
                      'ALTER TABLE "student" DROP COLUMN "gpa"'

        migration_op = "RemoveAttribute(entity_name='Course', attr_name='lab_hours')\n" \
                       "RemoveAttribute(entity_name='Student', attr_name='gpa')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_unique_attr(self):
        """
            Sets unique constraint to attribute "name" in entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100, unique=True)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "department" ADD CONSTRAINT "unq_department__name" UNIQUE ("name")'

        migration_op = "AddUniqueConstraint(entity_name='Department', attr_name='name')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_unset_unique_attr(self):
        """
            Unsets unique constraint from attribute "major" in entity "Group"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "group" DROP CONSTRAINT "unq_group__major"'

        migration_op = "DropUniqueConstraint(entity_name='Group', attr_name='major')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_unsigned_attr(self):
        """
            Set's unsigned constraint to attribute "lect_hours" in entity "Course"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int, unsigned=True)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeColumnType(entity_name='Course', attr_name='lect_hours', py_type=int, options={'unsigned': True})"

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "lect_hours" TYPE BIGINT'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_unset_unsigned_attr_1(self):
        """
            Unset's unsigned constraint to attribute "lab_hours" in entity "Course"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeColumnType(entity_name='Course', attr_name='lab_hours', py_type=int, options={})"

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "lab_hours" TYPE INTEGER'

        # test execution freezes at apply_migrate() call
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_unset_unsigned_attr_2(self):
        """
            Unset's unsigned constraint to attribute "lab_hours" in entity "Course", specify smaller size
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, size=8)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeColumnType(entity_name='Course', attr_name='lab_hours', py_type=int, options={'size': 8})"

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "lab_hours" TYPE SMALLINT'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_default_val(self):
        """
            Set's default value to attribute "name" in entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100, default="deptName")
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = ''

        migration_op = ""

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_database_column_val(self):
        """
            Set's database column value to attribute "name" in entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100, column='mydept')
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "department" RENAME COLUMN "name" TO "mydept"'

        migration_op = "RenameColumns(entity_name='Department', attr_name='name', new_columns_names=['mydept'])"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_sql_type_val(self):
        """
            Set's sql type "smallint" to attribute "semester" in entity "Course"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int, sql_type="smallint")
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "lect_hours" TYPE smallint'

        migration_op = "ChangeColumnType(entity_name='Course', attr_name='lect_hours', py_type=int, options={'sql_type': 'smallint'})"
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_sql_default_val(self):
        """
            Set's sql default parameter to attribute "name" in entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str, sql_default='empty_name')
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "teacher" ALTER COLUMN "name" SET DEFAULT \'empty_name\''

        migration_op = "ChangeSQLDefault(entity_name='Teacher', attr_name='name', sql_default='empty_name')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_decimal_precision(self):
        """
            Set's precision parameter to attributes "rating" in entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal, precision=10)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "department" ALTER COLUMN "rating" TYPE DECIMAL(10, 2)'
        migration_op = "ChangeColumnType(entity_name='Department', attr_name='rating', py_type=Decimal, options={'precision': 10})"
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_datetime_precision(self):
        """
            Set's precision parameter to attribute "last_update" in entity "Course"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime, precision=5)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "last_update" TYPE TIMESTAMP(5)'
        migration_op = "ChangeColumnType(entity_name='Course', attr_name='last_update', py_type=datetime, options={'precision': 5})"
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_rel_attr_req_to_opt(self):
        """
            Changes required attribute "teacher" in entity "Course" to optional
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Optional('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = '\n'.join([
            'ALTER TABLE "course" DROP CONSTRAINT "fk_course__teacher_id"',
            'ALTER TABLE "course" ALTER COLUMN "teacher_id" DROP NOT NULL',
            'ALTER TABLE "course" ADD CONSTRAINT "fk_course__teacher_id" '
                'FOREIGN KEY ("teacher_id") REFERENCES "teacher" ("id") ON DELETE SET NULL'
        ])
        migration_op = "ChangeAttributeClass(entity_name='Course', attr_name='teacher', new_class='Optional')"
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_rel_attr_opt_to_req(self):
        """
            Changes optional attribute "curator" in entity "Group" to required
        """
        # Incorrect migration operation for required attr with initial value
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Required('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeAttributeClass(entity_name='Group', attr_name='curator', new_class='Required')"

        correct_sql = 'ALTER TABLE "group" DROP CONSTRAINT "fk_group__curator_id"\n' \
                      'ALTER TABLE "group" ALTER COLUMN "curator_id" SET NOT NULL\n' \
                      'ALTER TABLE "group" ADD CONSTRAINT "fk_group__curator_id" FOREIGN KEY ("curator_id") ' \
                      'REFERENCES "teacher" ("id") ON DELETE CASCADE'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema) # Test fails on schemas comparing

    def test_change_primary_key_to_required(self):
        """
            Changes primary key attribute "number" in entity "Department" to required
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = Required(int)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        with self.assertRaises(MigrationError) as cm:
            self.apply_migrate()

        self.assertEqual(cm.exception.args[0], 'Cannot change primary key')

    def test_change_primary_key_to_optional(self):
        """
            Changes primary key attribute "number" in entity "Department" to optional
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = Optional(int)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        with self.assertRaises(MigrationError) as cm:
            self.apply_migrate()

        self.assertEqual(cm.exception.args[0], 'Cannot change primary key')

    def test_change_required_to_optional(self):
        """
            Changes required attribute "lect_hours" in entity "Course" to optional
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Optional(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "lect_hours" DROP NOT NULL'

        migration_op = "ChangeAttributeClass(entity_name='Course', attr_name='lect_hours', new_class='Optional')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_required_to_primary_key(self):
        """
            Erases old primary key in entity "Course" and makes attribute "name" primary key
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = PrimaryKey(str)
            semester = Required(int)
            lect_hours = Optional(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        with self.assertRaises(MigrationError) as cm:
            self.apply_migrate()

        self.assertEqual(cm.exception.args[0], 'Cannot change primary key')

    def test_change_optional_to_required_1a(self):
        """
            Changes optional attribute "description" in entity "Course" to required without initial value
        """
        # Incorrect migration operation for required attr with initial value
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Required(str, initial="Empty description")
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeAttributeClass(entity_name='Course', attr_name='description', " \
                                            "new_class='Required', initial='Empty description')"

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "description" DROP DEFAULT\n' \
                      'UPDATE "course"\n' \
                      'SET "description" = \'Empty description\'\n' \
                      'WHERE ("description" = \'\' OR "description" IS NULL)'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema) # Test fails on schemas comparing

    def test_change_optional_to_required_1b(self):
        """
            Changes optional attribute "description" in entity "Course" to required without initial value
        """
        # Incorrect migration operation for required attr with initial value
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Required(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeAttributeClass(entity_name='Course', attr_name='description', new_class='Required')"

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "description" DROP DEFAULT'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema) # Test fails on schemas comparing

    def test_change_optional_to_required_2a(self):
        """
            Changes optional nullable attribute "raiting" in entity "Departemnt" to required with initial value
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Required(Decimal, initial=Decimal('1.0'))

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group)
            head_of_dept = Optional('DeptDirector')

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeAttributeClass(entity_name='Department', attr_name='rating', new_class='Required', initial=Decimal('1.0'))" \


        correct_sql = 'ALTER TABLE "department" ALTER COLUMN "rating" SET NOT NULL\n' \
                      'UPDATE "department"\n' \
                      'SET "rating" = 1.0\n' \
                      'WHERE "rating" IS NULL'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema) # Test fails on schemas comparing

    def test_change_optional_to_required_2b(self):
        """
            Changes optional nullable attribute "raiting" in entity "Departemnt" to required without initial value
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Required(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group)
            head_of_dept = Optional('DeptDirector')

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        migration_op = "ChangeAttributeClass(entity_name='Department', attr_name='rating', new_class='Required')"

        correct_sql = 'ALTER TABLE "department" ALTER COLUMN "rating" SET NOT NULL'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema) # Test fails on schemas comparing

    def test_change_optional_to_primary_key(self):
        """
            Erases old primary key in entity "Course" and makes optional attribute "description" primary key
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            description = PrimaryKey(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        with self.assertRaises(MigrationError) as cm:
            self.apply_migrate()

        self.assertEqual(cm.exception.args[0], 'Cannot change primary key')

    def test_set_inherits_from_entity(self):
        """
            Set entity "Group" inherited from entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required('Group')
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set('Group')
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class Group(Teacher):
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        with self.assertRaises(MigrationError) as cm:
            self.apply_migrate()

        self.assertEqual(cm.exception.args[0], 'Cannot change primary key')

    def test_add_relation_opt_to_opt(self):
        """
            Adds optional-optional relation in entity "Department"(attribute head_of_dept) and entity "DeptDirector"
            (attribute "dept")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)
            head_of_dept = Optional('DeptDirector')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)
            dept = Optional(Department)

        correct_sql = 'ALTER TABLE "department" ADD COLUMN "head_of_dept_id" INTEGER\n' \
                      'ALTER TABLE "department" ADD CONSTRAINT "fk_department__head_of_dept_id" FOREIGN KEY ' \
                      '("head_of_dept_id") REFERENCES "teacher" ("id") ON DELETE SET NULL\n' \
                      'CREATE INDEX "idx_department__head_of_dept_id" ON "department" ("head_of_dept_id")'

        migration_op = "AddRelation(entity1_name='Department', attr1=Optional('head_of_dept', 'DeptDirector'), " \
                       "entity2_name='DeptDirector', attr2=Optional('dept', 'Department'))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_relation_opt_to_req(self):
        """
            Adds optional-required relation in entity "Department"(attribute head_of_dept) and entity "DeptDirector"
            (attribute "dept")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)
            head_of_dept = Optional('DeptDirector')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)
            dept = Required(Department)

        correct_sql = 'ALTER TABLE "teacher" ADD COLUMN "dept_number" INTEGER\n' \
                      'ALTER TABLE "teacher" ADD CONSTRAINT "fk_teacher__dept_number" FOREIGN KEY ("dept_number") ' \
                      'REFERENCES "department" ("number") ON DELETE CASCADE\n' \
                      'CREATE INDEX "idx_teacher__dept_number" ON "teacher" ("dept_number")'

        migration_op = "AddRelation(entity1_name='Department', attr1=Optional('head_of_dept', 'DeptDirector'), " \
                       "entity2_name='DeptDirector', attr2=Required('dept', 'Department')) "
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_relation_req_to_opt(self):
        """
            Adds required-optional relation in entity "Department"(attribute head_of_dept) and entity "DeptDirector"
            (attribute "dept")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)
            head_of_dept = Required('DeptDirector', initial=1)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)
            dept = Optional(Department)

        correct_sql = ''

        migration_op = ""
        # apply_migrate raises exception: Error
        # Traceback (most recent call last):
        #   File "/usr/lib64/python3.7/unittest/case.py", line 59, in testPartExecutor
        #     yield
        #   File "/usr/lib64/python3.7/unittest/case.py", line 628, in run
        #     testMethod()
        #   File "/home/admin/pony/pony/orm/migrations/tests/test_ops.py", line 2716, in test_add_relation_req_to_opt
        #     expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        #   File "/home/admin/pony/pony/orm/migrations/tests/test_ops.py", line 81, in apply_migrate
        #     self.db2.generate_mapping(check_tables=False)
        #   File "/home/admin/pony/pony/orm/core.py", line 1002, in generate_mapping
        #     database.vdb = VirtualDB.from_db(database)
        #   File "/home/admin/pony/pony/orm/migrations/virtuals.py", line 22, in from_db
        #     self.entities[name] = VirtualEntity.from_entity(self, db.entities[name])
        #   File "/home/admin/pony/pony/orm/migrations/virtuals.py", line 221, in from_entity
        #     v_attr = VirtualAttribute.from_attribute(attr)
        #   File "/home/admin/pony/pony/orm/migrations/virtuals.py", line 452, in from_attribute
        #     vattr = attr_class(name, py_type, *attr.given_args['args'], reverse=reverse, **attr.given_args['kwargs'])
        #   File "/home/admin/pony/pony/orm/migrations/virtuals.py", line 337, in __init__
        #     throw(core.MappingError, "initial option cannot be used in relation")
        #   File "/home/admin/pony/pony/utils/utils.py", line 129, in throw
        #     raise exc
        # pony.orm.core.MappingError: initial option cannot be used in relation
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_relation_set_to_req(self):
        """
            Adds set-required relation in entity "Department"(attribute "students") and entity "Student"
            (attribute "dept")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)
            students = Set('Student')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')
            dept = Required(Department, initial=0)  # TODO

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)
        # last command appear twice
        correct_sql = 'ALTER TABLE "student" ADD COLUMN "dept_number" INTEGER NOT NULL\n' \
                      'ALTER TABLE "student" ADD CONSTRAINT "fk_student__dept_number" FOREIGN KEY ("dept_number") ' \
                      'REFERENCES "department" ("number") ON DELETE CASCADE\n' \
                      'CREATE INDEX "idx_student__dept_number" ON "student" ("dept_number")\n' \
                      'ALTER TABLE "student" ALTER COLUMN "dept_number" DROP DEFAULT'

        migration_op = "AddRelation(entity1_name='Department', attr1=Set('students', 'Student'), " \
                       "entity2_name='Student', attr2=Required('dept', 'Department', initial=0))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_relation_req_to_set(self):
        """
            Adds required-set relation in entity "Department"(attribute "representative") and entity "Student"
            (attribute "depts")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)
            representative = Required('Student', initial=0)  # TODO

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')
            depts = Set(Department)

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)
        # last command appear twice
        correct_sql = 'ALTER TABLE "department" ADD COLUMN "representative_id" INTEGER NOT NULL\n' \
                      'ALTER TABLE "department" ADD CONSTRAINT "fk_department__representative_id" ' \
                      'FOREIGN KEY ("representative_id") REFERENCES "student" ("id") ON DELETE CASCADE\n' \
                      'CREATE INDEX "idx_department__representative_id" ON "department" ("representative_id")\n' \
                      'ALTER TABLE "department" ALTER COLUMN "representative_id" DROP DEFAULT'

        migration_op = "AddRelation(entity1_name='Department', " \
                       "attr1=Required('representative', 'Student', initial=0), " \
                       "entity2_name='Student', attr2=Set('depts', 'Department'))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_relation_set_to_opt(self):
        """
            Adds set-optional relation in entity "Department"(attribute "students") and entity "Student"
            (attribute "dept")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)
            students = Set('Student')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')
            dept = Optional(Department)

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "student" ADD COLUMN "dept_number" INTEGER\n' \
                      'ALTER TABLE "student" ADD CONSTRAINT "fk_student__dept_number" FOREIGN KEY ("dept_number") ' \
                      'REFERENCES "department" ("number") ON DELETE SET NULL\n' \
                      'CREATE INDEX "idx_student__dept_number" ON "student" ("dept_number")'

        migration_op = "AddRelation(entity1_name='Department', attr1=Set('students', 'Student'), " \
                       "entity2_name='Student', attr2=Optional('dept', 'Department'))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_relation_opt_to_set(self):
        """
            Adds optional-set relation in entity "Department"(attribute "representative") and entity "Student"
            (attribute "depts")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)
            representative = Optional('Student')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')
            depts = Set(Department)

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "department" ADD COLUMN "representative_id" INTEGER\n' \
                      'ALTER TABLE "department" ADD CONSTRAINT "fk_department__representative_id" ' \
                      'FOREIGN KEY ("representative_id") REFERENCES "student" ("id") ON DELETE SET NULL\n' \
                      'CREATE INDEX "idx_department__representative_id" ON "department" ("representative_id")'

        migration_op = "AddRelation(entity1_name='Department', attr1=Optional('representative', 'Student'), " \
                       "entity2_name='Student', attr2=Set('depts', 'Department'))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_relation_set_to_set(self):
        """
            Adds set-set relation in entity "Department"(attribute "students") and entity "Student"
            (attribute "depts")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)
            students = Set('Student')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')
            depts = Set(Department)


        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'CREATE TABLE "department_students" (\n' \
                      '  "department_number" INTEGER NOT NULL,\n' \
                      '  "student_id" INTEGER NOT NULL,\n' \
                      '  PRIMARY KEY ("department_number", "student_id")\n' \
                      ')\n' \
                      'CREATE INDEX "idx_department_students__department_number" ON ' \
                      '"department_students" ("department_number")\n' \
                      'CREATE INDEX "idx_department_students__student_id" ON "department_students" ("student_id")\n' \
                      'ALTER TABLE "department_students" ADD CONSTRAINT "fk_department_students__department_number" ' \
                      'FOREIGN KEY ("department_number") REFERENCES "department" ("number") ON DELETE CASCADE\n' \
                      'ALTER TABLE "department_students" ADD CONSTRAINT "fk_department_students__student_id" ' \
                      'FOREIGN KEY ("student_id") REFERENCES "student" ("id") ON DELETE CASCADE'

        migration_op = "AddRelation(entity1_name='Department', attr1=Set('students', 'Student'), " \
                       "entity2_name='Student', attr2=Set('depts', 'Department'))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_float_as_primary_key(self):
        """
            Adds entity "Mark" with float primary key
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        with self.assertRaises(TypeError) as cm:
            class Mark(db2.Entity):
                id = PrimaryKey(float)
                mark_value = Required(int)

        self.db2.generate_mapping(create_tables=False)
        self.assertEqual(cm.exception.args[0], 'PrimaryKey attribute Mark.id cannot be of type float')

    def test_add_relation_symmetric_self_reference(self):
        """
            Adds symmetric self-reference relation (attribute "friends") in entity "Student"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')
            friends = Set('Student', reverse='friends')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'CREATE TABLE "student_friends" (\n' \
                      '  "student_id" INTEGER NOT NULL,\n' \
                      '  "student_id_2" INTEGER NOT NULL,\n' \
                      '  PRIMARY KEY ("student_id", "student_id_2")\n' \
                      ')\n' \
                      'CREATE INDEX "idx_student_friends__student_id" ON "student_friends" ("student_id")\n' \
                      'CREATE INDEX "idx_student_friends__student_id_2" ON "student_friends" ("student_id_2")\n' \
                      'ALTER TABLE "student_friends" ADD CONSTRAINT "fk_student_friends__student_id" ' \
                      'FOREIGN KEY ("student_id") REFERENCES "student" ("id") ON DELETE CASCADE\n' \
                      'ALTER TABLE "student_friends" ADD CONSTRAINT "fk_student_friends__student_id_2" ' \
                      'FOREIGN KEY ("student_id_2") REFERENCES "student" ("id") ON DELETE CASCADE'

        migration_op = "AddSymmetricRelation(entity_name='Student', attr=Set('friends', 'Student', reverse='friends'))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_relation_self_reference(self):
        """
            Adds self-reference relation (attributes "parent" and "child") in entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)
            parent = Required('Teacher', reverse='child', initial=0)
            child = Optional('Teacher', reverse='parent')

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)
        # last command appear twice
        correct_sql = 'ALTER TABLE "teacher" ADD COLUMN "parent_id" INTEGER NOT NULL\n' \
                      'ALTER TABLE "teacher" ADD CONSTRAINT "fk_teacher__parent_id" FOREIGN KEY ("parent_id") ' \
                      'REFERENCES "teacher" ("id")\n' \
                      'CREATE INDEX "idx_teacher__parent_id" ON "teacher" ("parent_id")\n' \
                      'ALTER TABLE "teacher" ALTER COLUMN "parent_id" DROP DEFAULT\n' \
                      'ALTER TABLE "teacher" ALTER COLUMN "parent_id" DROP DEFAULT'

        migration_op = "AddRelation(entity1_name='Teacher', attr1=Required('parent', 'Teacher', initial=0), " \
                       "entity2_name='Teacher', attr2=Optional('child', 'Teacher'))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_relation_name(self):
        """
            Changes set-set relation name between entities "Teacher" and "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            tchrs = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            dprtmnts = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = ''

        migration_op = ""
        rename_map = {('Department', 'teachers'): ('Department', 'tchrs'),
                      ('Teacher', 'departments'): ('Teacher', 'dprtmnts')}
        # apply_migrate raises exception: Error
        # Traceback (most recent call last):
        #   File "/usr/lib64/python3.7/unittest/case.py", line 59, in testPartExecutor
        #     yield
        #   File "/usr/lib64/python3.7/unittest/case.py", line 628, in run
        #     testMethod()
        #   File "/home/admin/pony/pony/orm/migrations/tests/test_ops.py", line 3427, in test_change_relation_name
        #     expected_schema, actual_schema, migration, sql_ops = self.apply_migrate(rename_map=rename_map)
        #   File "/home/admin/pony/pony/orm/migrations/tests/test_ops.py", line 91, in apply_migrate
        #     op.apply(new_vdb)
        #   File "/home/admin/pony/pony/orm/migrations/operations.py", line 998, in apply
        #     self.apply_to_schema(vdb, attr, self.new_attr_name)
        #   File "/home/admin/pony/pony/orm/migrations/operations.py", line 1016, in apply_to_schema
        #     schema.rename_table(m2m_table.name, new_m2m_name)
        #   File "/home/admin/pony/pony/orm/migrations/dbschema.py", line 898, in rename_table
        #     del schema.tables[table.name]
        # AttributeError: 'Name' object has no attribute 'name'
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate(rename_map=rename_map)
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_relation_opt_to_opt(self):
        """
            Deletes attribute "head_of_dept" in entity "Teacher" and attribute "teacher" in entity "DeptDirector"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher', table="TeachToDepts")
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)

        correct_sql = 'ALTER TABLE "teacher" DROP COLUMN "teacher_id"'

        migration_op = "RemoveAttribute(entity_name='Teacher', attr_name='head_of_dept')\n" \
                       "RemoveAttribute(entity_name='DeptDirector', attr_name='teacher')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_relation_opt_to_req(self):
        """
            Deletes attribute "student" in entity "Teacher" and attribute "mentor" in entity "Student"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "student" DROP COLUMN "mentor_id"'

        migration_op = "RemoveAttribute(entity_name='Student', attr_name='mentor')\n" \
                       "RemoveAttribute(entity_name='Teacher', attr_name='student')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_relation_req_to_set(self):
        """
            Deletes attribute "dept" in entity "Group" and attribute "groups" in entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "group" DROP COLUMN "dept_number"'

        migration_op = "RemoveAttribute(entity_name='Department', attr_name='groups')\n" \
                       "RemoveAttribute(entity_name='Group', attr_name='dept')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_relation_opt_to_set(self):
        """
            Deletes relation attributes "curator" in entity "Group" and "groups" in entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "group" DROP COLUMN "curator_id"'

        migration_op = "RemoveAttribute(entity_name='Group', attr_name='curator')\n" \
                       "RemoveAttribute(entity_name=\'Teacher\', attr_name=\'groups\')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_relation_set_to_set(self):
        """
            Deletes set-set relation
            between entities "Department"(attribute "teachers") and "Teacher"(attribute "departments")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = ''

        migration_op = ""

        #apply_migrate() raises exception
        # Error
        # Traceback (most recent call last):
        #   File "/usr/lib64/python3.7/unittest/case.py", line 59, in testPartExecutor
        #     yield
        #   File "/usr/lib64/python3.7/unittest/case.py", line 628, in run
        #     testMethod()
        #   File "/home/admin/pony/pony/orm/migrations/tests/test_ops.py", line 3516, in test_delete_relation
        #     expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        #   File "/home/admin/pony/pony/orm/migrations/tests/test_ops.py", line 92, in apply_migrate
        #     op.apply(new_vdb)
        #   File "/home/admin/pony/pony/orm/migrations/operations.py", line 267, in apply
        #     self.apply_to_schema(vdb, attr)
        #   File "/home/admin/pony/pony/orm/migrations/operations.py", line 275, in apply_to_schema
        #     vdb.schema.drop_table(vdb.schema.tables[m2m_table_name])
        # KeyError: 'department_teachers'
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_intermediate_table_name(self):
        """
           Sets name for intermediate table of relation Department(attribute "teachers") -Teachers(attribute "departments")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher', table="TeachToDepts")
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "department_teachers" RENAME TO "TeachToDepts"\n' \
                      'ALTER TABLE "TeachToDepts" RENAME CONSTRAINT "fk_department_teachers__department_number" ' \
                          'TO "fk_teachtodepts__department_number"\n' \
                      'ALTER TABLE "TeachToDepts" RENAME CONSTRAINT "fk_department_teachers__teacher_id" ' \
                          'TO "fk_teachtodepts__teacher_id"\n' \
                      'ALTER INDEX "idx_department_teachers__department_number" RENAME ' \
                          'TO "idx_teachtodepts__department_number"\n' \
                      'ALTER INDEX "idx_department_teachers__teacher_id" RENAME TO "idx_teachtodepts__teacher_id"'

        migration_op = "RenameM2MTable(entity_name='Department', attr_name='teachers', new_name='TeachToDepts')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_intermediate_table_name_with_column_names(self):
        """
           Sets name for intermediate table of relation Department(attribute "teachers") -Teachers(attribute "departments")
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher', table="TeachToDepts", column="t_id")
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department, column="dept_id")
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "department_teachers" RENAME COLUMN "teacher_id" TO "t_id"\n' \
                      'ALTER TABLE "department_teachers" RENAME CONSTRAINT "fk_department_teachers__teacher_id" ' \
                          'TO "fk_department_teachers__t_id"\n' \
                      'ALTER INDEX "idx_department_teachers__teacher_id" RENAME TO "idx_department_teachers__t_id"\n' \
                      'ALTER TABLE "department_teachers" RENAME TO "TeachToDepts"\n' \
                      'ALTER TABLE "TeachToDepts" RENAME CONSTRAINT "fk_department_teachers__department_number" ' \
                          'TO "fk_teachtodepts__department_number"\n' \
                      'ALTER TABLE "TeachToDepts" RENAME CONSTRAINT "fk_department_teachers__t_id" ' \
                          'TO "fk_teachtodepts__t_id"\n' \
                      'ALTER INDEX "idx_department_teachers__department_number" RENAME ' \
                          'TO "idx_teachtodepts__department_number"\n' \
                      'ALTER INDEX "idx_department_teachers__t_id" RENAME TO "idx_teachtodepts__t_id"\n' \
                      'ALTER TABLE "TeachToDepts" RENAME COLUMN "department_number" TO "dept_id"\n' \
                      'ALTER TABLE "TeachToDepts" RENAME CONSTRAINT "fk_teachtodepts__department_number" ' \
                          'TO "fk_teachtodepts__dept_id"\n' \
                      'ALTER INDEX "idx_teachtodepts__department_number" RENAME TO "idx_teachtodepts__dept_id"'

        migration_op = "RenameColumns(entity_name='Department', attr_name='teachers', new_columns_names=['t_id'])\n" \
                       "RenameM2MTable(entity_name='Department', attr_name='teachers', new_name='TeachToDepts')\n" \
                       "RenameColumns(entity_name='Teacher', attr_name='departments', new_columns_names=['dept_id'])"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_scale(self):
        """
           Sets scale for attribute "rating" in entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal, scale=5)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "department" ALTER COLUMN "rating" TYPE DECIMAL(12, 5)'

        migration_op = "ChangeColumnType(entity_name='Department', attr_name='rating', py_type=Decimal, options={'scale': 5})"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_unset_scale(self):
        """
           Unsets scale for attribute "avg_mark" in entity "Course"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "course" ALTER COLUMN "avg_mark" TYPE DECIMAL(12, 2)'

        migration_op = "ChangeColumnType(entity_name='Course', attr_name='avg_mark', py_type=Decimal, options={})"
        # test execution freezes at apply_migrate() call
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_unset_cascade_delete(self):
        """
           Unsets parameter cascade_delete for attribute "groups" in entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "group" DROP CONSTRAINT "fk_group__curator_id"\n' \
                      'ALTER TABLE "group" ADD CONSTRAINT "fk_group__curator_id" FOREIGN KEY ("curator_id") ' \
                      'REFERENCES "teacher" ("id") ON DELETE SET NULL'

        migration_op = "ChangeCascadeDeleteOption(entity_name='Teacher', attr_name='groups', cascade_delete=None)"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_cascade_delete(self):
        """
           Sets parameter cascade_delete to True for attribute "head_of_dept" in entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector', cascade_delete=True)
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "teacher" DROP CONSTRAINT "fk_teacher__teacher_id"\n' \
                      'ALTER TABLE "teacher" ADD CONSTRAINT "fk_teacher__teacher_id" FOREIGN KEY ("teacher_id") ' \
                      'REFERENCES "teacher" ("id") ON DELETE CASCADE'

        migration_op = "ChangeCascadeDeleteOption(entity_name='Teacher', attr_name='head_of_dept', cascade_delete=True)"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_cascade_delete_for_set_attr(self):
        """
           Sets parameter cascade_delete to True for set attribute "courses" in entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course', cascade_delete=True)
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "course" DROP CONSTRAINT "fk_course__dept_number"\n' \
                      'ALTER TABLE "course" ADD CONSTRAINT "fk_course__dept_number" FOREIGN KEY ("dept_number") ' \
                      'REFERENCES "department" ("number") ON DELETE CASCADE'

        migration_op = "ChangeCascadeDeleteOption(entity_name='Department', attr_name='courses', cascade_delete=True)"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_index_true(self):
        """
           Sets parameter index to True for attribute "name" in entity "Student"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str, index=True)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'CREATE INDEX "idx_student__name" ON "student" ("name")'

        migration_op = "AddIndex(entity_name='Student', attr_name='name', index=True)"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_index_value(self):
        """
           Sets parameter index to "student_name" for attribute "name" in entity "Student"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str, index="student_name")
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'CREATE INDEX "student_name" ON "student" ("name")'

        migration_op = "AddIndex(entity_name='Student', attr_name='name', index='student_name')"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_index_false(self):
        """
           Sets parameter index to False for attribute "surname" in entity "Teacher"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=False)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = ''

        migration_op = ""
        # apply_migrate() raises exception
        # Error
        # Traceback (most recent call last):
        #   File "/usr/lib64/python3.7/unittest/case.py", line 59, in testPartExecutor
        #     yield
        #   File "/usr/lib64/python3.7/unittest/case.py", line 628, in run
        #     testMethod()
        #   File "/home/admin/pony/pony/orm/migrations/tests/test_ops.py", line 4670, in test_set_index_false
        #     expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        #   File "/home/admin/pony/pony/orm/migrations/tests/test_ops.py", line 95, in apply_migrate
        #     op.apply(new_vdb)
        #   File "/home/admin/pony/pony/orm/migrations/operations.py", line 1413, in apply
        #     self.apply_to_schema(vdb, attr)
        #   File "/home/admin/pony/pony/orm/migrations/operations.py", line 1422, in apply_to_schema
        #     vdb.schema.rename_index(index, attr.index)
        #   File "/home/admin/pony/pony/orm/migrations/dbschema.py", line 1214, in rename_index
        #     schema.ops.extend(index.get_rename_sql(new_name))
        #   File "/home/admin/pony/pony/orm/migrations/dbschema.py", line 28, in wrap
        #     res = func(obj, *args, **kwargs)
        #   File "/home/admin/pony/pony/orm/migrations/dbschema.py", line 618, in get_rename_sql
        #     result.append(quote(new_name))
        #   File "/home/admin/pony/pony/orm/dbapiprovider.py", line 262, in quote_name
        #     return '.'.join(provider.quote_name(item) for item in name)
        # TypeError: 'bool' object is not iterable
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_time_precision(self):
        """
           Sets parameter precision to "last_online" attribute in entity "Student"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time, precision=4)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'ALTER TABLE "student" ALTER COLUMN "last_online" TYPE TIME(4)'

        migration_op = 'ChangeColumnType(entity_name=\'Student\', attr_name=\'last_online\', ' \
                       'py_type=time, options={\'precision\': 4})'

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_incorrect_time_precision(self):
        """
           Sets incorrect precision to "last_online" attribute in entity "Student"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time, precision=8)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        with self.assertRaises(ValueError) as cm:
            self.apply_migrate()

        self.assertEqual(cm.exception.args[0], 'Precision value of attribute Student.last_online must be between '
                                               '0 and 6. Got: 8')

    def test_add_composite_index(self):
        """
            Add composite index (attributes "number" and "name") to entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, 100)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')
            rating = Optional(Decimal)
            composite_index(number, name)

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')
            curator = Optional('Teacher')

        class Course(db2.Entity):
            name = Required(str)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            avg_mark = Optional(Decimal, scale=8)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)
            last_update = Optional(datetime)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            last_online = Optional(time)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            mentor = Required('Teacher')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str, index=True)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)
            groups = Set(Group, cascade_delete=True)
            head_of_dept = Optional('DeptDirector')
            student = Optional(Student)

        class DeptDirector(Teacher):
            is_director = Required(bool)
            teacher = Optional(Teacher)

        correct_sql = 'CREATE INDEX "idx_department__number__name" ON "department" ("number", "name")'

        migration_op = "AddCompositeIndex(entity_name='Department', attr_names=('number', 'name'))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual(expected_schema, actual_schema)
