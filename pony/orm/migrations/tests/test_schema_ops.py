import unittest
from datetime import date
from pony.orm import *
from pony.orm.migrations import VirtualDB, Migration


class AbstractTestMigrations(unittest.TestCase):
    db_params = {}

    @classmethod
    def setUpClass(cls):
        if cls is AbstractTestMigrations:
            raise unittest.SkipTest("Skipping base class tests")
        super(AbstractTestMigrations, cls).setUpClass()

    def setUp(self):
        self.db = Database(**self.db_params)
        db = self.db

        class Department(db.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

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
            actual_schema.apply(connection, False, False)

        return expected_schema, actual_schema, m

    def test_equal_schemas(self):
        self.db2 = self.db
        vdb2 = VirtualDB.from_db(self.db)
        vdb2.schema = self.db.provider.vdbschema_cls.from_vdb(vdb2, self.db.provider)
        self.assertEqual(self.db.vdb.schema, vdb2.schema)

    def test_add_entity(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            marks = Set('Mark')
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            marks = Set('Mark')
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        class Mark(db2.Entity):
            value = Required(int)
            dt = Required(date)
            course = Required(Course)
            student = Required(Student)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # AddEntity
        self.assertEqual(expected_schema, actual_schema)

    def test_remove_add_attribute(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            # credits = Required(int, index=True)
            exam = Optional(Json)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 2)  # RemoveAttribute, AddAttribute
        self.assertEqual(expected_schema, actual_schema)

    def test_add_relation(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")
            students = Set('Student')

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)
            department = Optional(Department)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # AddRelation
        self.assertEqual(expected_schema, actual_schema)

    def test_remove_entity(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            # courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        # class Course(db2.Entity):
        #     name = Required(str)
        #     semester = Required(int)
        #     lect_hours = Required(int, check='lect_hours > 0')
        #     lab_hours = Required(int)
        #     credits = Required(int, index=True)
        #     dept = Required(Department)
        #     students = Set("Student")
        #     PrimaryKey(name, semester)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            # courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # RemoveEntity
        self.assertEqual(expected_schema, actual_schema)

    def test_rename_table(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            _table_ = 'Pupil'
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # RenameTable
        self.assertEqual(expected_schema, actual_schema)

    def test_rename_columns(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department, column='department')
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course, columns=['c_name', 'c_semester'])
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 2)  # RenameColumns
        self.assertEqual(expected_schema, actual_schema)

    def test_delete_composite_key(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            # composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # DeleteCompositeKey
        self.assertEqual(expected_schema, actual_schema)

    def test_add_composite_key(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")
            composite_key(number, major)

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migrations = self.apply_migrate()
        self.assertEqual(len(migrations.operations), 1)  # AddCompositeKey
        self.assertEqual(expected_schema, actual_schema)

    def test_drop_composite_index(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            # composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # DropCompositeIndex
        self.assertEqual(expected_schema, actual_schema)

    def test_add_composite_index(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)
            composite_index(name, dob, group)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # AddCompositeIndex
        self.assertEqual(expected_schema, actual_schema)

    def test_rename_entity(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Subject")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Subject(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Subject)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        rename_map = {'Course': 'Subject'}
        expected_schema, actual_schema, migration = self.apply_migrate(rename_map)
        self.assertEqual(len(migration.operations), 1)  # RenameEntity
        self.assertEqual(expected_schema, actual_schema)

    def test_rename_entity_2(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group2")
            courses = Set("Course")

        class Group2(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group2)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        rename_map = {'Group': 'Group2'}
        expected_schema, actual_schema, migration = self.apply_migrate(rename_map)
        self.assertEqual(len(migration.operations), 1)
        self.assertEqual(expected_schema, actual_schema)

    def test_rename_attribute(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            fullname = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(fullname, dob)

        class NewStudent(Student):
            avatar = Required(str)

        rename_map = {('Student', 'name'): ('Student', 'fullname')}
        expected_schema, actual_schema, migration = self.apply_migrate(rename_map)
        self.assertEqual(len(migration.operations), 1)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_column_type(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(int)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)
        self.assertEqual(expected_schema, actual_schema)

    def test_change_sql_default(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str, sql_default='427-06-03')
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # ChangeSQLDefault
        self.assertEqual(expected_schema, actual_schema)

    def test_change_nullable(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True, nullable=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # ChangeNullable
        self.assertEqual(expected_schema, actual_schema)

    def test_rename_m2m_table(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course, table='student_course')
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # RenameM2MTable
        self.assertEqual(expected_schema, actual_schema)

    def test_add_unique_constraint(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str, unique=True)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # AddUniqueConstraint
        self.assertEqual(expected_schema, actual_schema)

    def test_remove_unique_constraint(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # RemoveUniqueConstraint
        self.assertEqual(expected_schema, actual_schema)

    def test_add_check_constraint(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int, check='number > 0')
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # AddCheckConstraint
        self.assertEqual(expected_schema, actual_schema)

    @unittest.skipIf(db_params and db_params['provider'] == 'sqlite', 'Cannot drop check constraint in SQLite')
    def test_drop_check_constraint(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int)
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)
        self.assertEqual(expected_schema, actual_schema)

    def test_add_index(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str, index=True)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)
        self.assertEqual(expected_schema, actual_schema)

    def test_rename_index(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index='major_index')
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # RenameIndex
        self.assertEqual(expected_schema, actual_schema)

    def test_drop_index(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # DropIndex
        self.assertEqual(expected_schema, actual_schema)

    def test_rename_foreign_key(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group, fk_name='group_fk')
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        try:
            expected_schema, actual_schema, migration = self.apply_migrate()
        except NotImplementedError as e:
            self.assertTrue(self.db_params['provider'] == 'sqlite' and
                            str(e) == 'Renaming foreign key is not implemented for SQLite')
            return
        self.assertEqual(len(migration.operations), 1)  # RenameForeignKey
        self.assertEqual(expected_schema, actual_schema)

    def test_change_cascade_delete_1(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student", cascade_delete=False)

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # ChangeCascadeDeleteOption
        self.assertEqual(expected_schema, actual_schema)

    def test_change_cascade_delete_2(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student", cascade_delete=False)

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # ChangeCascadeDeleteOption
        self.assertEqual(expected_schema, actual_schema)

    def test_change_discriminator(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int)
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            _discriminator_ = 'stud_disrc'
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # ChangeDiscriminator
        self.assertEqual(expected_schema, actual_schema)


class TestPostgresMigrations(AbstractTestMigrations):
    db_params = dict(provider='postgres', user='pony', password='pony', host='localhost', database='pony')


class TestMySQLMigrations(AbstractTestMigrations):
    db_params = dict(provider='mysql', user='pony', password='pony', host='localhost', database='pony')


class TestOracleMigrations(AbstractTestMigrations):
    db_params = dict(provider='oracle', user='C##PONY', password='pony', dsn='172.17.0.2:1521/ORCLCDB')

    def test_add_check_constraint(self):
        self.db2 = db2 = Database(**self.db_params)

        class Department(db2.Entity):
            number = PrimaryKey(int, auto=True)
            name = Required(str, unique=True)
            groups = Set("Group")
            courses = Set("Course")

        class Group(db2.Entity):
            number = PrimaryKey(int, check='"NUMBER" > 0')
            major = Required(str, index=True)
            dept = Required("Department", fk_name='dept_fk')
            students = Set("Student")

        class Course(db2.Entity):
            name = Required(str, unique=True)
            semester = Required(int)
            lect_hours = Required(int, check='lect_hours > 0')
            lab_hours = Required(int)
            credits = Required(int, index=True)
            dept = Required(Department)
            students = Set("Student")
            PrimaryKey(name, semester)
            composite_index(lect_hours, lab_hours)

        class Student(db2.Entity):
            name = Required(str)
            dob = Required(date)
            tel = Optional(str)
            picture = Optional(buffer, lazy=True)
            gpa = Required(float, default=0)
            group = Required(Group)
            courses = Set(Course)
            composite_key(name, dob)

        class NewStudent(Student):
            avatar = Required(str)

        expected_schema, actual_schema, migration = self.apply_migrate()
        self.assertEqual(len(migration.operations), 1)  # AddCheckConstraint
        self.assertEqual(expected_schema, actual_schema)


class TestSQLiteMigrations(AbstractTestMigrations):
    db_params = dict(provider='sqlite', filename='migrate_test.db', create_db=True)
