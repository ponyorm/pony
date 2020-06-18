import unittest
from datetime import date
from pony.orm import *
from pony.orm.migrations import VirtualDB, Migration
from collections import defaultdict


class TestMigrations(unittest.TestCase):
    db_params = dict(provider='postgres', user='pony', password='pony', host='localhost', database='pony')

    def setUp(self):
        self.db = Database(**self.db_params)
        db = self.db

        class Department(db.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')

        class Group(db.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')

        class Course(db.Entity):
            name = Required(str, 100)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int)
            credits = Required(int, size=8)
            dept = Required(Department)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)

        class Student(db.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)

        class Teacher(db.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)

        class DeptDirector(Teacher):
            is_director = Required(bool)

        db.generate_mapping(create_tables=True)

    def tearDown(self):
        self.db2.drop_all_tables()

    def apply_migrate(self, rename_map=None):
        self.db2.generate_mapping(check_tables=False)
        base_vdb = self.db.vdb
        tmp_vdb = self.db2.vdb

        new_vdb = VirtualDB.from_db(self.db)
        new_vdb.schema = self.db.provider.vdbschema_cls.from_vdb(new_vdb, self.db.provider)

        m = Migration.make(base_vdb, tmp_vdb, rename_map)
        new_vdb.vdb_only = False
        for op in m.operations:
            op.apply(new_vdb)
        expected_schema = tmp_vdb.schema
        actual_schema = new_vdb.schema
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
            name = Required(str)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')

        class Course(db2.Entity):
            name = Required(str, 100)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int)
            credits = Required(int, size=8)
            dept = Required(Department)
            students = Set('Student')
            teacher = Required('Teacher')
            course_mark = Optional('Course_mark')
            PrimaryKey(name, semester)
            description = Optional(str)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            picture = Optional(buffer)
            gpa = Optional(float)
            group = Required(Group)
            courses = Set(Course)
            course_marks = Set('Course_mark')

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)

        class DeptDirector(Teacher):
            is_director = Required(bool)

        class Course_mark(db2.Entity):  # added entity
            id = PrimaryKey(int, auto=True)
            course = Required(Course)
            student = Required(Student)
            mark = Required(int)

        correct_sql = 'CREATE TABLE "course_mark" (\n  ' \
                      '"id" SERIAL PRIMARY KEY,\n  ' \
                      '"course_name" VARCHAR(100) NOT NULL,\n  ' \
                      '"course_semester" INTEGER NOT NULL,\n  ' \
                      '"student_id" INTEGER NOT NULL,\n  ' \
                      '"mark" INTEGER NOT NULL\n)\n' \
                      'CREATE INDEX "idx_course_mark__course_name__course_semester" ON ' \
                      '"course_mark" ("course_name", "course_semester")\n' \
                      'CREATE INDEX "idx_course_mark__student_id" ON "course_mark" ("student_id")\n' \
                      'ALTER TABLE "course_mark" ADD CONSTRAINT "fk_course_mark__course_name__course_semester" ' \
                      'FOREIGN KEY ("course_name", "course_semester") REFERENCES "course" ("name", "semester")\n' \
                      'ALTER TABLE "course_mark" ADD CONSTRAINT "fk_course_mark__student_id" ' \
                      'FOREIGN KEY ("student_id") REFERENCES "student" ("id") ON DELETE CASCADE'

        migration_op = "AddEntity(Entity('Course_mark',  attrs=[\n        " \
                       "PrimaryKey('id', int, auto=True), \n        " \
                       "Required('course', 'Course', reverse='course_mark'), \n        " \
                       "Required('student', 'Student', reverse='course_marks'), \n        " \
                       "Required('mark', int)]))"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_inherited_entity(self):
        """
            Adds inherited entity "Graduate"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')

        class Course(db2.Entity):
            name = Required(str, 100)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int)
            credits = Required(int, size=8)
            dept = Required(Department)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)

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

        class DeptDirector(Teacher):
            is_director = Required(bool)

        class Graduate(Student):  # added inherited entity
            doc_number = Required(str)
            exam_mark = Required(int)

        correct_sql = 'ALTER TABLE "student" ADD COLUMN "doc_number" TEXT\n' \
                      'ALTER TABLE "student" ADD COLUMN "exam_mark" INTEGER'

        migration_op = "AddEntity(Entity('Graduate', bases=['Student'], attrs=[\n        " \
                       "Required('doc_number', str), \n        " \
                       "Required('exam_mark', int)]))"

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
            name = Required(str)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str, unique=True)
            dept = Required(Department)
            students = Set('Student')

        class Course(db2.Entity):
            name = Required(str, 100)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int)
            credits = Required(int, size=8)
            dept = Required(Department)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)

        class Pupil(db2.Entity):
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

        class DeptDirector(Teacher):
            is_director = Required(bool)

        correct_sql = ''

        migration_op = ""

        rename_map = {'Student': 'Pupil'}
        # TODO apply_migrate() returns error: pony.orm.core.ERDiagramError: Entity definition Student was not found
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
            name = Required(str)
            courses = Set('Course')
            teachers = Set('Teacher')

        class Course(db2.Entity):
            name = Required(str, 100)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int)
            credits = Required(int, size=8)
            dept = Required(Department)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str)

        class Student(db2.Entity):
            id = PrimaryKey(int, auto=True)
            name = Required(str)
            dob = Required(date)
            picture = Optional(buffer)
            gpa = Optional(float)
            courses = Set(Course)

        class Teacher(db2.Entity):
            id = PrimaryKey(int)
            name = Required(str)
            surname = Optional(str)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)

        class DeptDirector(Teacher):
            is_director = Required(bool)

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








