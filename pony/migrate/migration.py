from __future__ import unicode_literals
from pony.py23compat import ExitStack, suppress

import re, os, os.path
from datetime import datetime
from textwrap import dedent

import pony
from pony import orm

from . import get_cmd_exitstack, get_migration_dir
from .exceptions import MergeAborted
from .graph import MigrationGraph
from .operations import CustomOp
from .serializer import serializer_factory
from .utils import run_path
from .writer import MigrationWriter, MIGRATION_TEMPLATE


class Migration(object):
    def __repr__(self):
        return repr(self.operations)

    def __init__(self, name=None, loader=None):
        self.name = name
        self.loader = loader
        self.operations = []
        self.dependencies = []

    @classmethod
    def make_entity(cls, db):
        db.migration_in_progress = True
        class Migration(db.Entity):
            name = orm.Required(str)
            applied = orm.Required(datetime)


    @classmethod
    def _generate_name(cls, loader, name=None):
        graph = loader.graph
        highest_number = 0
        for leaf in graph.leaf_nodes():
            num = cls._parse_number(leaf)
            if num is not None and num > highest_number:
                highest_number = num
        highest_number += 1
        if highest_number == 1:
            return "0001_initial"
        if name is None:
            name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return "%04i_%s" % (highest_number, name)


    @classmethod
    def _parse_number(cls, name):
        """
        Given a migration name, tries to extract a number from the
        beginning of it. If no number found, returns None.
        """
        match = re.match(r'^\d+', name)
        if match:
            return int(match.group())
        return None



    @classmethod
    def make(cls, db, empty=False, custom=False, filename=None):
        get_cmd_exitstack().callback(db.disconnect)
        loader = MigrationLoader()
        leaves = loader.graph.leaf_nodes()
        if len(leaves) > 1:
            from pony.migrate import command
            command.merge(db, loader, leaves)
            return
        migrations = get_migration_dir()
        if not os.path.exists(migrations):
            os.mkdir(migrations)
        if not leaves:
            generated = MigrationWriter(leaves, None, db).as_string()
            name = '0001_initial'
            p = os.path.join(migrations, '{}.py'.format(name))
            with open(p, 'w') as f:
                f.write(generated)
            print('Written: %s' % os.path.relpath(p))
            return

        [leaf] = leaves
        plan = loader.graph.forwards_plan(leaf)
        initial = plan[0]
        p = os.path.join(migrations, '{}.py'.format(initial))
        define_entities = run_path(p)['define_entities']
        prev_db = cls._reconstruct(db)
        get_cmd_exitstack().callback(prev_db.disconnect)
        define_entities(prev_db)
        prev_db.generate_schema()
        for name in plan[1:]:
            mod = cls._read_file(name)
            cls._upgrade_db(prev_db, mod)

        db.generate_schema()
        prev_db.generate_schema()

        name = cls._generate_name(loader, filename)
        if empty:
            template_data = {
                'op_kwargs': 'forward=forward',
                'final_state_func': '',
            }

            if custom:
                template_data['op_kwargs'] = 'forward=forward, final_state=final_state'
                writer = MigrationWriter(None, None, db)
                final_state_func = writer._get_define_entities_block(
                        set(), func_name='final_state')
                final_state_func = '\n'.join(('', final_state_func, ''))
                template_data['final_state_func'] = final_state_func

            imports = {
                'from pony.orm import *',
                'from pony.migrate import diagram_ops as op',
            }
            database, im = serializer_factory(db).serialize()
            imports.update(im)
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
                'deps': str(leaves),
                'version': pony.__version__,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M"),
            }
            generated = MIGRATION_TEMPLATE.format(**ctx)
            p = os.path.join(migrations, '{}.py'.format(name))
            with open(p, 'w') as f:
                f.write(generated)
            print('Written: %s' % os.path.relpath(p))
            return

        writer = MigrationWriter(leaves, prev_db, db)
        generated = writer.as_string()
        if not writer.operations:
            print('No changes.')
            return

        p = os.path.join(migrations, '{}.py'.format(name))
        with open(p, 'w') as f:
            f.write(generated)
        print('Written: %s' % os.path.relpath(p))


    @classmethod
    def _reconstruct(cls, db):
        args, kwargs = db._constructor_args
        return db.__class__(*args, **kwargs)

    @classmethod
    def _read_db(cls, name, orig_db, db_cache=None):
        if db_cache is None:
            # cache will not be used
            db_cache = {}
        db = db_cache.get(name)
        if db:
            return db
        db = cls._reconstruct(orig_db)
        p = os.path.join(
            get_migration_dir(), '{}.py'.format(name)
        )
        define_entities = run_path(p)['define_entities']
        get_cmd_exitstack().callback(db.disconnect)
        define_entities(db)
        Migration.make_entity(db)
        db_cache[name] = db
        return db

    @classmethod
    def _read_file(cls, name):
        p = os.path.join(
           get_migration_dir(), '{}.py'.format(name)
        )
        return run_path(p)

    @classmethod
    def _upgrade_db(cls, db, module):
        if module.get('operations') is None:
            module['define_entities'](db)
            db.generate_schema() # ?
            return
        for op in module['operations']:
            op.apply(db)
        return module['operations']


    @classmethod
    def apply(cls, db, dry_run=False, is_fake=False, name_start=None, name_end=None,
              name_exact=None):
        get_cmd_exitstack().callback(db.disconnect)
        # temporary, for tests
        # here should be assert db.schema
        if not db.schema:
            db.schema = db.generate_schema()

        assert not 'Migration' in db.entities
        migration_db = cls._reconstruct(db)
        Migration.make_entity(migration_db)
        migration_db.generate_mapping(create_tables=True, check_tables=True)
        get_cmd_exitstack().callback(migration_db.disconnect)

        loader = MigrationLoader()
        leaves = loader.graph.leaf_nodes()
        if len(leaves) > 1:
            try:
                from . import command
                command.merge(db, loader, leaves)
            except MergeAborted:
                return
            loader.build_graph()
            leaves = loader.graph.leaf_nodes()
            assert len(leaves) == 1
        assert leaves
        leaf = leaves[0]
        forwards_plan = loader.graph.forwards_plan(leaf)
        names = forwards_plan
        if name_exact:
            start_index = names.index(name_exact)
            names = [names[start_index]]
        elif name_end:
            start_index = names.index(name_start) if name_start else None
            names = names[start_index:names.index(name_end)+1]

        applied = []
        with ExitStack() as stack:
            stack.enter_context(orm.db_session)
            if dry_run:
                stack.enter_context(suppress(orm.DatabaseError))
            applied = orm.select(m.name for m in migration_db.Migration)[:]
            applied = sorted(applied, key=lambda name: forwards_plan.index(name))

        if all(name in applied for name in names):
            print('\nAll migrations are applied.')
            return

        prev_db = cls._reconstruct(db)
        new_db = cls._reconstruct(db)
        get_cmd_exitstack().callback(prev_db.disconnect)
        get_cmd_exitstack().callback(new_db.disconnect)

        prev_mod = None

        if applied:
            for i, name in enumerate(applied):
                mod = cls._read_file(name)
                if i:
                    cls._upgrade_db(prev_db, prev_mod)
                cls._upgrade_db(new_db, mod)
                prev_mod = mod

        op_batches = []
        applied_names = []

        def generate(new_db, prev_db, entity_ops, current_schema=db.schema):
            current_schema = db.schema
            schema = new_db.generate_schema()
            prev_schema = prev_db and prev_db.generate_schema()

            # TODO remove ?
            operations = [o for batch in op_batches for o in batch]

            from pony.migrate.executor import Executor
            ops = Executor(schema, prev_schema,
                           prev_db=prev_db, db=new_db, current_schema=current_schema,
                           entity_ops=entity_ops, operations=operations
                           )
            return ops.generate()

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

        def execute(ops, db):
            if len(ops) == 1 and isinstance(ops[0], CustomOp):
                with orm.db_session:
                    ops[0].func(db)
                return
            for op in ops:
                cls.execute(op, db, dry_run)

        for i, name in enumerate(names):
            if name in applied:
                continue
            mod = cls._read_file(name)
            if mod.get('operations') is None:
                batches = [mod]
            else:
                batches = [
                    {'operations': ops} for ops in split_ops(mod['operations'])
                ]
            for dic in batches:
                if prev_mod is not None:
                    cls._upgrade_db(prev_db, prev_mod)
                cls._upgrade_db(new_db, dic)
                prev_mod = dic

                if dic.get('operations') and dic['operations'][0].is_custom:
                    ops = dic['operations']
                    op = CustomOp(ops[0].forward)
                    to_execute = [op]
                else:
                    to_execute = generate(new_db, prev_db, dic.get('operations'))

                if not (is_fake and name == '0001_initial'):
                    # execute in transaction
                    @orm.db_session(ddl=True)
                    def run():
                        prev_db_schema = prev_db.schema
                        # FIXME is generated 2nd time
                        prev_db.schema = prev_db.generate_schema()
                        try:
                            execute(to_execute, prev_db)
                        finally:
                            prev_db.schema = prev_db_schema

                    run()

            applied_names.append(name)

        with orm.db_session:
            if not dry_run:
                for name in applied_names:
                    migration_db.Migration(name=name, applied=datetime.now())

        print('Applied:')
        for name in applied_names:
            print('  - %s' % name)


    @classmethod
    def execute(cls, op, db, dry_run=False, applied_ops=None):
        sql = op.get_sql().strip()
        if not sql:
            return
        if not dry_run:
            db.execute(sql)
        else:
            print(sql)


class MigrationLoader(object):

    def __init__(self):
        self.build_graph()

    def load_disk(self):
        # Discover .py files
        self.disk_migrations = {}
        directory = get_migration_dir()
        if not os.path.exists(directory):
            return
        migration_files = {
            name for name in os.listdir(directory)
            if name.endswith('.py')
        }
        # Read them
        for migration_name in migration_files:
            migrations = get_migration_dir()
            py_file = os.path.join(migrations, migration_name)
            ns = run_path(py_file)
            migration_name = migration_name.rsplit(".", 1)[0]
            mig = Migration(migration_name, self)
            mig.dependencies = ns['dependencies']
            self.disk_migrations[migration_name] = mig

    def add_internal_dependencies(self, key, migration):
        # TODO see what is key
        for parent in migration.dependencies:
            if parent[0] != key[0] or parent[1] == '__first__':
                # Ignore __first__ references to the same app (#22325).
                continue
            self.graph.add_dependency(migration, key, parent, skip_validation=True)

    def build_graph(self):
        # Load disk data
        self.load_disk()

        self.graph = MigrationGraph()
        for key, migration in self.disk_migrations.items():
            self.graph.add_node(key, migration)
            # Internal (aka same-app) dependencies.
            self.add_internal_dependencies(key, migration)

        self.graph.validate_consistency()

