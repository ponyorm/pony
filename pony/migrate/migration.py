from __future__ import unicode_literals
from pony.py23compat import ExitStack

import re, os, os.path
from datetime import datetime
from textwrap import dedent
from glob import glob

import pony
from pony import orm
from pony.utils import throw

from . import get_cmd_exitstack, get_migration_dir
from .exceptions import MergeAborted, MigrationFileCorrupted, \
    MigrationDirectoryNotFound, UnmergedMigrationsDetected, \
    MigrationFileNotFound, MultipleMigrationFilesFound
from .operations import CustomOp
from .serializer import serialize
from .utils import run_path
from .writer import MigrationWriter, MIGRATION_TEMPLATE
from .executor import Executor

def make_migration_entity(db):
    db.migration_in_progress = True
    class Migration(db.Entity):
        name = orm.Required(str)
        applied = orm.Required(datetime)

def parse_number(name):
    """
    Given a migration name, tries to extract a number from the
    beginning of it. If no number found, returns None.
    """
    match = re.match(r'^\d+', name)
    if match:
        return int(match.group())
    return None

def run_migration_file(name):
    path = os.path.join(
       get_migration_dir(), '{}.py'.format(name)
    )
    return run_path(path)


def upgrade_db(db, module):
    operations = module.get('operations', [])
    define_entities = module.get('define_entities')
    if define_entities:
        define_entities(db)
        db.generate_schema()
    for op in operations:
        op.apply(db)

def reconstruct_db(db):
    args, kwargs = db._constructor_args
    return db.__class__(*args, **kwargs)


class Migration(object):
    def __init__(self, name, graph):
        self.is_applied = False
        self.name = name
        self.graph = graph
        self.number = parse_number(name)
        self.operations = []
        self.dependencies = []
        self.parents = []
        self.children = []

    def __repr__(self):
        return 'Migration(%r)' % self.name

    def add_child(self, child):
        if child not in self.children:
            self.children.append(child)

    def add_parent(self, parent):
        if parent not in self.parents:
            self.parents.append(parent)

    def forwards_list(self):
        stack = [self]
        migration_set = set(stack)
        resolved = set()
        result = []
        while stack:
            top = stack[-1]
            if top not in resolved:
                parents = [
                    parent for parent in reversed(top.parents)
                    if parent not in migration_set
                ]
                stack.extend(parents)
                migration_set.update(parents)
                resolved.add(top)
            else:
                top = stack.pop()
                result.append(top)
        return result

    def backwards_list(self):
        stack = [self]
        migration_set = set(stack)
        resolved = set()
        result = []
        while stack:
            top = stack[-1]
            if top not in resolved:
                children = [
                    child for child in top.children
                    if child not in migration_set
                ]
                stack.extend(children)
                migration_set.update(children)
                resolved.add(top)
            else:
                top = stack.pop()
                result.append(top)
        return result


class MigrationGraph(object):

    def __init__(self):
        self.migrations = {}
        directory = get_migration_dir()
        if not os.path.exists(directory):
            throw(MigrationDirectoryNotFound, directory)

        migration_pathnames = glob(os.path.join(directory, '*.py'))
        for pathname in migration_pathnames:
            namespace = run_path(pathname)
            migration_name = os.path.basename(pathname)[:-3]
            migration = Migration(migration_name, self)
            if 'dependencies' not in namespace: throw(MigrationFileCorrupted,
                '`dependencies` list was not found in migration file %s' % pathname)

            migration.dependencies = namespace['dependencies']
            self.migrations[migration_name] = migration

        for name, migration in self.migrations.items():
            for dependency in migration.dependencies:
                parent = self.migrations.get(dependency)
                if not parent: throw(MigrationFileCorrupted,
                     'Dependency %s for migration %s was not found'
                     % (dependency, name))

                migration.add_parent(parent)
                parent.add_child(migration)

    def get_migration_by_prefix(self, prefix):
        migration_names = [name for name in self.migrations if name.startswith(prefix)]
        if len(migration_names) > 1:
            throw(MultipleMigrationFilesFound, migration_names)
        if not migration_names:
            throw(MigrationFileNotFound, prefix)
        return self.migrations[migration_names[0]]

    def get_next_migration_number(self):
        try:
            return max(m.number for m in self.migrations.values() if m.number is not None) + 1
        except ValueError:
            return 1

    def make_next_migration_name(self, description=None, with_timestamp=False):
        number = self.get_next_migration_number()
        if description is None and number == 1:
            description = 'initial'
        if with_timestamp or description is None:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            description = timestamp if not description else '%s_%s' % (description, timestamp)
        return "%04i_%s" % (number, description)

    def roots(self):
        return [m for m in self.migrations.values() if not m.parents]

    def leaves(self):
        return [m for m in self.migrations.values() if not m.children]

    def find_cycle(self):
        todo = set(self.migrations.values())
        while todo:
            node = todo.pop()
            stack = [node]
            while stack:
                top = stack[-1]
                for node in top.children:
                    if node in stack:
                        return stack[stack.index(node):]
                    if node in todo:
                        stack.append(node)
                        todo.remove(node)
                        break
                else:
                    node = stack.pop()
        return None

    def merge(self):
        leaves = self.leaves()
        assert len(leaves) > 1
        migration_name = self.make_next_migration_name('merge', with_timestamp=True)
        ctx = {
            'deps': leaves,
            'version': pony.__version__,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M"),
            'body': 'operations = []',
            'imports': '',
        }
        generated = MIGRATION_TEMPLATE.format(**ctx)
        self._write_migration_file(migration_name, generated)

    @staticmethod
    def _write_migration_file(name, generated):
        path = os.path.join(get_migration_dir(), '%s.py' % name)
        with open(path, 'w') as f:
            f.write(generated)
        filename = os.path.relpath(path)
        print('Written: %s' % filename)
        return filename

    def make(self, db, empty=False, custom=False, description=None):
        get_cmd_exitstack().callback(db.disconnect)
        leaves = self.leaves()
        if len(leaves) > 1: throw(UnmergedMigrationsDetected, leaves)

        if not leaves:
            generated = MigrationWriter(leaves, None, db).as_string()
            return self._write_migration_file('0001_initial', generated)

        [leaf] = leaves
        plan = leaf.forwards_list()
        initial = plan[0]
        path = os.path.join(get_migration_dir(), '%s.py' % initial.name)
        define_entities = run_path(path)['define_entities']
        prev_db = reconstruct_db(db)
        get_cmd_exitstack().callback(prev_db.disconnect)
        define_entities(prev_db)
        prev_db.generate_schema()
        for migration in plan[1:]:
            namespace = run_migration_file(migration.name)
            upgrade_db(prev_db, namespace)

        db.generate_schema()
        prev_db.generate_schema()

        name = self.make_next_migration_name(description)
        if empty:
            template_data = {
                'op_kwargs': 'forward=forward',
                'final_state_func': '',
            }

            if custom:
                template_data['op_kwargs'] = 'forward=forward, final_state=final_state'
                writer = MigrationWriter(None, None, db)
                final_state_func = writer._get_define_entities_block(set(), func_name='final_state')
                template_data['final_state_func'] = '\n%s\n' % final_state_func

            imports = {
                'from pony.orm import *',
                'from pony.migrate import diagram_ops as op',
            }
            database = serialize(db, imports=imports)
            imports.discard('from pony import orm')

            body = dedent('''
            def forward(db):
                pass
            {final_state_func}
            operations = [
                op.Custom({op_kwargs})
            ]
            ''').format(**template_data)
            imports = sorted(imports, key=lambda i: i.split()[1])
            imports = '\n'.join(imports)
            ctx = {
                'database': database,
                'body': body,
                'imports': imports,
                'deps': leaves,
                'version': pony.__version__,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
            generated = MIGRATION_TEMPLATE.format(**ctx)
            return self._write_migration_file(name, generated)

        writer = MigrationWriter(leaves, prev_db, db)
        generated = writer.as_string()
        if not writer.operations:
            print('No changes.')
            return

        return self._write_migration_file(name, generated)

    def apply(self, db, dry_run=False, is_fake=False, name_start=None, name_end=None,
              name_exact=None):
        get_cmd_exitstack().callback(db.disconnect)
        # temporary, for tests
        # here should be assert db.schema
        if not db.schema:
            db.schema = db.generate_schema()

        assert not 'Migration' in db.entities
        migration_db = reconstruct_db(db)
        make_migration_entity(migration_db)
        migration_db.generate_mapping(create_tables=True, check_tables=True)
        get_cmd_exitstack().callback(migration_db.disconnect)

        leaves = self.leaves()
        if len(leaves) > 1: throw(UnmergedMigrationsDetected, leaves)
        if not leaves:
            print('\nNo migrations were found.')
            return

        leaf = leaves[0]
        forwards_plan = leaf.forwards_list()
        if name_exact:
            migration = self.get_migration_by_prefix(name_exact)
            assert migration in forwards_plan
            migrations = [migration]
        elif name_end:
            start_index = forwards_plan.index(self.get_migration_by_prefix(name_start)) if name_start else None
            end_index = forwards_plan.index(self.get_migration_by_prefix(name_end))
            migrations = forwards_plan[start_index:end_index+1]
        else:
            migrations = forwards_plan

        with orm.db_session:
            applied = set(orm.select(m.name for m in migration_db.Migration))

        applied_migrations = [m for m in migrations if m.name in applied]
        migrations_to_apply = [m for m in migrations if m.name not in applied]

        if not migrations_to_apply:
            print('\nAll migrations are applied.')
            return

        prev_db = reconstruct_db(db)
        new_db = reconstruct_db(db)
        get_cmd_exitstack().callback(prev_db.disconnect)
        get_cmd_exitstack().callback(new_db.disconnect)

        if applied_migrations:
            namespace = None
            for migration in applied_migrations:
                namespace = run_migration_file(migration.name)
                upgrade_db(prev_db, namespace)
                upgrade_db(new_db, namespace)

        op_batches = []
        applied_names = []

        def split_ops(entity_ops):
            group = []
            for op in entity_ops:
                if op.is_custom:
                    if group:
                        yield group
                        group = []
                    yield [op]

                else:
                    group.append(op)
            if group:
                yield group

        if not dry_run:
            print('Applied:')

        prev_dic = None
        for migration in migrations_to_apply:
            namespace = run_migration_file(migration.name)
            if namespace.get('operations') is None:
                batches = [namespace]
            else:
                batches = [
                    {'operations': ops} for ops in split_ops(namespace['operations'])
                ]
            for dic in batches:
                if prev_dic is not None:
                    upgrade_db(prev_db, prev_dic)
                upgrade_db(new_db, dic)
                prev_dic = dic

                ops = dic.get('operations', ())
                if ops and ops[0].is_custom:
                    op = CustomOp(ops[0].forward)
                    to_execute = [op]
                else:
                    executor = Executor(prev_db, new_db, ops)
                    to_execute = executor.generate()

                with orm.db_session(ddl=True):
                    if not (is_fake and migration.name == '0001_initial'):
                        prev_db_schema = prev_db.schema
                        # FIXME is generated 2nd time
                        prev_db.schema = prev_db.generate_schema()
                        if len(to_execute) == 1 and isinstance(to_execute[0], CustomOp):
                            to_execute[0].func(prev_db)
                        else:
                            for op in to_execute:
                                sql = op.get_sql().strip()
                                if sql:
                                    if dry_run:
                                        print(sql)
                                    else:
                                        prev_db.execute(sql)
                        prev_db.schema = prev_db_schema
                        prev_db.commit()

                    if not dry_run:
                        migration_db.Migration(name=migration.name, applied=datetime.now())
                        print('  - %s' % migration.name)

            applied_names.append(migration.name)
