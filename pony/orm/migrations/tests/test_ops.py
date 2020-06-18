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
            lab_hours = Required(int, unsigned=True)
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
            lab_hours = Required(int, unsigned=True)
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
            lab_hours = Required(int, unsigned=True)
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
            lab_hours = Required(int, unsigned=True)
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
            lab_hours = Required(int, unsigned=True)
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

    def test_delete_inherited_entity(self):
        """
            Delete entity "DeptDirector"
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
            lab_hours = Required(int, unsigned=True)
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

        correct_sql = ''

        migration_op = ""
        # TODO apply_migrate() returns error: KeyError: 'classtype' from method table.columns.pop(col_name)
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_any_table_name(self):
        """
            Set's table name "dept" to entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            _table_ = 'dept'
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
            lab_hours = Required(int, unsigned=True)
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
            code = Required(str, initial="00.00.00")
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
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
            lab_hours = Required(int, unsigned=True)
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
            patronymic = Optional(str)
            dob = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)

        class DeptDirector(Teacher):
            is_director = Required(bool)

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
            lab_hours = Required(int, unsigned=True)
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
            date_of_birth = Required(date)
            departments = Set(Department)
            courses = Set(Course)
            biography = Optional(str, nullable=True)

        class DeptDirector(Teacher):
            is_director = Required(bool)

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
            Change max_length parameter for string attribute. Attribute "name" in entity "Course"
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
            name = Required(str, 300)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
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

        correct_sql = ''

        migration_op = ""
        # TODO getting error from apply_migrate:
        """
        Last SQL: ALTER TABLE "course" ALTER COLUMN "name" TYPE VARCHAR(300) USING

        Error
        Traceback (most recent call last):
          File "/home/admin/pony/pony/orm/dbapiprovider.py", line 55, in wrap_dbapi_exceptions
            return func(provider, *args, **kwargs)
          File "/home/admin/pony/pony/orm/dbproviders/postgres.py", line 294, in execute
            if arguments is None: cursor.execute(sql)
        psycopg2.errors.SyntaxError: syntax error at end of input
        LINE 1: ...R TABLE "course" ALTER COLUMN "name" TYPE VARCHAR(300) USING
                                                                               ^


        During handling of the above exception, another exception occurred:

        Traceback (most recent call last):
          File "/home/admin/pony/pony/orm/migrations/dbschema.py", line 1690, in apply
            schema.provider.execute(cursor, op.sql)
          File "<string>", line 2, in execute
          File "/home/admin/pony/pony/orm/dbapiprovider.py", line 67, in wrap_dbapi_exceptions
            raise ProgrammingError(e)
        pony.orm.dbapiprovider.ProgrammingError: syntax error at end of input
        LINE 1: ...R TABLE "course" ALTER COLUMN "name" TYPE VARCHAR(300) USING
        """
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_int_size(self):
        """
            Change int attribute size. Attribute "credits" in entity "Course"
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
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=32)
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

        correct_sql = ''

        migration_op = ""
        # TODO getting error from apply_migrate:
        """
        Last SQL: ALTER TABLE "course" ALTER COLUMN "credits" TYPE INTEGER USING

        Error
        Traceback (most recent call last):
          File "/home/admin/pony/pony/orm/dbapiprovider.py", line 55, in wrap_dbapi_exceptions
            return func(provider, *args, **kwargs)
          File "/home/admin/pony/pony/orm/dbproviders/postgres.py", line 294, in execute
            if arguments is None: cursor.execute(sql)
        psycopg2.errors.SyntaxError: syntax error at end of input
        LINE 1: ...TER TABLE "course" ALTER COLUMN "credits" TYPE INTEGER USING
                                                                               ^


        During handling of the above exception, another exception occurred:

        Traceback (most recent call last):
          File "/home/admin/pony/pony/orm/migrations/dbschema.py", line 1690, in apply
            schema.provider.execute(cursor, op.sql)
          File "<string>", line 2, in execute
          File "/home/admin/pony/pony/orm/dbapiprovider.py", line 67, in wrap_dbapi_exceptions
            raise ProgrammingError(e)
        pony.orm.dbapiprovider.ProgrammingError: syntax error at end of input
        LINE 1: ...TER TABLE "course" ALTER COLUMN "credits" TYPE INTEGER USING
                                                                               ^
        """
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    @unittest.skip
    def test_unset_nullable_attr(self):
        """
            Unset's "nullable" to attribute "biography" in entity "Teacher"
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
            lab_hours = Required(int, unsigned=True)
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
            biography = Optional(str)

        class DeptDirector(Teacher):
            is_director = Required(bool)

        correct_sql = ''

        migration_op = ""
        # test execution freezes at apply_migrate() call
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_nullable_attr(self):
        """
            Set's "nullable" to attribute "name" in entity "Teacher"
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
            lab_hours = Required(int, unsigned=True)
            credits = Required(int, size=8)
            dept = Required(Department)
            students = Set('Student')
            teacher = Required('Teacher')
            PrimaryKey(name, semester)
            description = Optional(str, nullable=True)

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

        correct_sql = ''

        migration_op = "ChangeNullable(entity_name='Course', attr_name='description', nullable=True)"
        # different expected and actual schemas
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_attr(self):
        """
            Delete's attribute: optional "gpa" from entity "Student" and required "lab_hours" from "Course"
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
            name = Required(str, unique=True)
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
            lab_hours = Required(int, unsigned=True)
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
            name = Required(str)
            groups = Set('Group')
            courses = Set('Course')
            teachers = Set('Teacher')

        class Group(db2.Entity):
            number = PrimaryKey(int, auto=True)
            major = Required(str)
            dept = Required(Department)
            students = Set('Student')

        class Course(db2.Entity):
            name = Required(str, 100)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
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
            lect_hours = Required(int, unsigned=True)
            lab_hours = Required(int, unsigned=True)
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

        correct_sql = ''

        migration_op = "ChangeColumnType(entity_name='Course', attr_name='lect_hours', new_options={'unsigned': True})"

        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    @unittest.skip
    def test_unset_unsigned_attr(self):
        """
            Unset's unsigned constraint to attribute "lab_hours" in entity "Course"
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

        correct_sql = ''

        migration_op = ""
        # test execution freezes at apply_migrate() call
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)

    def test_set_default_val(self):
        """
            Set's default value to attribute "name" in entity "Department"
        """
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, default="deptName")
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
            lab_hours = Required(int, unsigned=True)
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
            name = Required(str, column='mydept')
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
            lab_hours = Required(int, unsigned=True)
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
            semester = Required(int, sql_type="smallint")
            lect_hours = Required(int)
            lab_hours = Required(int, unsigned=True)
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

        correct_sql = ''

        migration_op = ""
        # apply_migrate raises exception
        # psycopg2.errors.SyntaxError: syntax error at end of input
        # LINE 1: ...R TABLE "course" ALTER COLUMN "semester" TYPE smallint USING
        expected_schema, actual_schema, migration, sql_ops = self.apply_migrate()
        imports = defaultdict(set)
        t = []
        for op in migration.operations:
            t.append(op.serialize(imports))

        self.assertEqual("\n".join(sql_ops), correct_sql)
        self.assertEqual("\n".join(t), migration_op)
        self.assertEqual(expected_schema, actual_schema)







