from __future__ import print_function

import sys, argparse
from collections import defaultdict

from pony.utils import throw

from pony.orm.migrations import Migration
from pony.orm.migrations import operations as ops
from pony.orm.migrations import virtuals as v



parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(help='commands', dest='cmd')

make_parser = subparsers.add_parser('make', help='make migration')
make_parser.add_argument('name', nargs='?', action='store')
make_parser.add_argument('--empty', action='store_true', help='create new migration file with no operations')
make_parser.add_argument('--data', action='store_true', help='make new data migration')

apply_parser = subparsers.add_parser('apply', help='apply unapplied migrations')
apply_parser.add_argument('--upto', nargs='?', action='store', help='final migration to be applied')
apply_parser.add_argument('--fake-initial', action='store_true', help='mark migration as applied')
apply_parser.add_argument('-v', '--verbose', action='store_true', help='show sql of applied migrations')

sql_parser = subparsers.add_parser('sql', help='show sql of migrations')
sql_parser.add_argument('--upto', action='store', help='final migration to be applied')
sql_parser.add_argument('--fake-initial', action='store_true', help='skip initial migration')

list_parser = subparsers.add_parser('list', help='list migrations')
list_parser.add_argument('--applied', action='store_true', help='show applied migrations too')

rename_parser = subparsers.add_parser('rename', help='rename entity or attribute')
rename_parser.add_argument('rename', nargs='+', action='store')

squash_parser = subparsers.add_parser('squash', help='squash all migrations into the new one')

upgrade_parser = subparsers.add_parser('upgrade', help='upgrade database to current Pony version')
upgrade_parser.add_argument('--sql-only', action='store_true', help='only show sql of upgrade process')
upgrade_parser.add_argument('-v', '--verbose', action='store_true', help='show sql of upgrade process')

downgrade_parser = subparsers.add_parser('downgrade', help='downgrade database to Pony version 0.7.11')
downgrade_parser.add_argument('--sql-only', action='store_true', help='only show sql of downgrade process')
downgrade_parser.add_argument('-v', '--verbose', action='store_true', help='show sql of downgrade process')


def migrate(db, cmd=None):
    if cmd is None:
        args = parser.parse_args().__dict__
        cmd = args.pop('cmd')
    else:
        args = defaultdict(type(None))
    from pony.orm.migrations.graph import MigrationGraph
    graph = MigrationGraph(db.migrations_dir)
    if cmd == 'make':
        make(db, graph, args)
    elif cmd in ('apply', 'sql'):
        vdb = v.VirtualDB(db.migrations_dir, db.provider)
        vdb.schema = vdb.provider.vdbschema_cls(vdb, vdb.provider)
        db.vdb = vdb
        apply(vdb, db, graph, args, sql_only=cmd == 'sql')
    elif cmd == 'rename':
        rename(db, graph, args)
    elif cmd == 'list':
        m_list(db, graph, args)
    elif cmd == 'squash':
        squash(db, graph, args)
    elif cmd == 'upgrade':
        upgrade(db, args)
    elif cmd == 'downgrade':
        downgrade(db, args)
    # elif cmd == 'apply':
    #     apply(db, graph, args, provider)


def get_applied(db):
    from pony.orm import commit
    vdb = db.vdb
    cache = db._get_cache()
    connection = cache.prepare_connection_for_query_execution()
    cursor = connection.cursor()

    if not vdb.provider.table_exists(connection, 'migration', False):
        create_query = vdb.schema.create_migration_table_sql()
        vdb.provider.execute(cursor, create_query)
        commit()

    applied_query = vdb.schema.get_applied_sql()
    vdb.provider.execute(cursor, applied_query)
    return [m[0] for m in cursor.fetchall()]


def apply(vdb, db, graph, args, sql_only=False):
    from pony.orm import Database, sql_debug
    def apply_(db, migration):
        vdb = db.vdb
        if migration.name in applied:
            for op in migration.operations:
                op.apply(vdb)
            vdb.validate()
            return

        if migration.data:
            old_db = Database()
            vdb.to_db(old_db)
            old_db.provider = db.provider
            old_db.generate_mapping(create_tables=False, check_tables=False)
            #  if args['verbose']:
            #     sql_debug(True)
            migration.data_migration(old_db)
            #  sql_debug(False)
            migration.save_applied(db)
            return

        if migration.based_on and any(m in applied for m in migration.based_on):
            if any(m not in applied for m in migration.based_on):
                for m_name in migration.based_on:
                    m = graph.squashed.get(m_name)
                    if m is None:
                        throw(MigrationError, 'Squashed migration %r was not found on disk' % m_name)
                    apply_(db, m)
            migration.save_applied(db, vdb.schema.errors)
            return

        if vdb.vdb_only:
            vdb.vdb_only = False
            vdb.schema = vdb.provider.vdbschema_cls.from_vdb(vdb, vdb.provider)

        for op in migration.operations:
            op.apply(vdb)
        vdb.validate()

        if not migration.dependencies and skip_intial:
            migration.save_applied(db, 0)
            return

        schema = vdb.schema
        with db_session:
            connection = db.get_connection()
            try:
                schema.apply(connection, verbose, sql_only)
            except Exception as e:
                print('During applying migration %r occurred exception %s: %s' %
                      (migration.name, e.__class__.__name__, e), file=sys.stderr)
                raise
            if not sql_only:
                migration.save_applied(db, schema.errors)
            schema.errors = 0

    from pony.orm import db_session, MigrationError
    route = graph.make_route()
    skip_intial = args['fake_initial']
    up_to = args['upto']
    verbose = args.get('verbose', False)

    with db_session:
        applied = get_applied(db)

    m_names = [m.name for m in route]

    if applied == m_names:
        print('All migrations are applied')
        return

    if up_to in applied:
        print('Migration %s already applied' % up_to)

    if up_to and up_to not in m_names:
        print('Invalid `upto` value: %s' % up_to)
        return

    for i, migration in enumerate(route):
        if migration.name == up_to:
            break
        apply_(db, migration)


def make(db, graph, args):
    from pony.orm.core import MigrationError
    vdb_curr = v.VirtualDB.from_db(db)
    vdb_prev = v.VirtualDB()
    vdb_prev.provider = vdb_curr.provider

    if not graph.migrations:
        # this is initial migration
        if args['name'] is not None:
            throw(MigrationError, 'Cannot set name for initial migration')
        if args['empty']:
            throw(MigrationError, 'Cannot make empty migration before initial')
        if args['data']:
            throw(MigrationError, 'Make initial migration before data')
        migration = Migration.make(vdb_prev, vdb_curr)
        # vdb_prev.init()
        if not migration.operations:
            print('Nothing to save')
            return
        migration.save(0, 'initial')
    else:
        route = graph.make_route()
        if args['empty'] or args['data']:
            migration = Migration(db.migrations_dir)
        else:
            for m in route:
                for op in m.operations:
                    op.apply(vdb_prev)
            migration = Migration.make(vdb_prev, vdb_curr)
            # for entity in vdb_prev.entities.values():
            #     entity.init(vdb_prev)
            if not migration.operations:
                print("No differences")
                return

        migration.dependencies.append(route[-1].name)
        migration.save(int(route[-1].name[:5]) + 1, args['name'], args['data'] or False)
    provider = db.provider_name
    not_implemented = [op.__class__.__name__ for op in migration.operations if provider in op.not_implemented]
    if not_implemented:
        print('Warning: next operations are not implemented for %s. '
              'Please provide custom SQL to apply these operations' % provider)
        for op in not_implemented:
            print('- %s' % op)
        print()
    print("Migration %s saved" % migration.name)


def rename(db, graph, args):
    from pony.orm.core import MigrationError
    renames = args['rename']
    if not renames:
        print("Nothing to rename")
        return
    if not graph.migrations:
        print("Make initial migration before")
        return
    route = graph.make_route()

    vdb_prev = v.VirtualDB()

    for m in route:
        for op in m.operations:
            op.apply(vdb_prev)

    vdb_curr = v.VirtualDB.from_db(db)

    migration = Migration(vdb_curr.migrations_dir, dependencies=[route[-1].name])
    for rename in renames:
        if ':' not in rename:
            throw(MigrationError, 'Incorrect rename value (%s). Use rename From:To' % rename)

        old, new = rename.split(':', 1)
        if '.' in old and '.' in new:
            # rename attribute
            entity1, attr1 = old.split('.', 1)
            entity2, attr2 = new.split('.', 1)
            if entity1 != entity2:
                # we should find if RenameEntity with these names are part of this migration
                for op in migration.operations:
                    if isinstance(op, ops.RenameEntity) and op.entity_name == entity1 and op.new_entity_name == entity2:
                        break  # found it
                    else:
                        throw(MigrationError, 'Incorrect usage of rename: %s.%s to %s.%s' %
                              (entity1, attr1, entity2, attr2))

            if not (attr1 in vdb_prev.entities[entity1].new_attrs and
                    attr2 in vdb_curr.entities[entity2].new_attrs):
                throw(MigrationError, 'Incorrect attribute rename %s' % rename)

            migration.operations.append(ops.RenameAttribute(entity2, attr1, attr2))

        elif '.' not in old and '.' not in new:
            # rename entity
            entity1 = old
            entity2 = new
            if entity1 not in vdb_prev.entities:
                throw(MigrationError, 'Entity %s was not found in a previous state' % entity1)
            if entity2 not in vdb_curr.entities:
                if entity2 in vdb_prev.entities:
                    # user forgot to rename
                    throw(MigrationError, 'Rename %s to %s in models file before using migrate rename' %
                          (entity1, entity2))
                throw(MigrationError, 'Entity %s not found' % entity2)
            migration.operations.append(ops.RenameEntity(entity1, entity2))
        else:
            throw(MigrationError, 'Incorrect usage of rename command (%s)' % rename)

    migration.save(int(route[-1].name[:5]) + 1)
    print("Migration %s saved" % migration.name)


def m_list(db, graph, args):
    if args['applied']:
        from pony.orm import core
        with core.db_session:
            applied = get_applied(db)
        if len(applied) == 0:
            print('No migrations were applied')
        else:
            print('All migrations')
        for m in graph.make_route():
            print(
                '+' if m.name in applied else '-',
                m.name
            )
    else:
        print('All migrations')
        for m in graph.make_route():
            print(m.name)


def squash(db, graph, args):
    from pony.orm.migrations.migrate import make_difference
    route = graph.make_route()
    if len(route) == 1:
        print('Nothing to squash')
        return
    result = []
    based_on = []
    rename_map = {}
    curr_db = v.VirtualDB()
    curr_db.provider = db.provider
    prev_db = v.VirtualDB()
    prev_db.provider = db.provider
    for m in route:
        based_on.append(m.name)
        for op in m.operations:
            if not op.sql:
                if isinstance(op, (ops.RenameEntity, ops.RenameAttribute)):
                    old_name = op.entity_name if isinstance(op, ops.RenameEntity) else (op.entity_name, op.attr_name)
                    new_name = op.new_entity_name if isinstance(op, ops.RenameEntity) else (
                    op.entity_name, op.new_attr_name)
                    for k, v in rename_map.items():
                        if old_name == v:
                            old_name = k
                            break
                    if old_name == new_name:
                        del rename_map[old_name]
                    else:
                        rename_map[old_name] = new_name
                elif isinstance(op, ops.RemoveEntity):
                    rename_map.pop(op.entity_name, None)
                elif isinstance(op, ops.RemoveAttribute):
                    rename_map.pop((op.entity_name, op.attr_name), None)
                op.apply(curr_db)
            else:
                ops = make_difference(prev_db, curr_db, rename_map)
                assert not rename_map
                result.extend(ops)
                result.append(op)
                for x_op in ops:
                    x_op.apply(prev_db)
                op.apply(prev_db)
                op.apply(curr_db)

    ops = make_difference(prev_db, curr_db, rename_map)
    # assert not rename_map
    result.extend(ops)

    migration = Migration(db.migrations_dir)
    migration.operations = result
    migration.based_on = based_on
    migration.save(int(route[-1].name[:5]) + 1, name='squashed')
    print("Squashed %d migrations. Migration %r saved" % (len(route), migration.name))


def upgrade(db, args):
    from pony.orm import db_session, commit
    vdb = db.vdb
    schema = vdb.schema
    with db_session:
        connection = db.get_connection()
        cursor = connection.cursor()
        res = schema.check_table_exists('pony_version', connection)
        if res is None:
            db.vdb.provider.execute(cursor, schema.create_upgrade_table_sql())
            db.vdb.provider.execute(cursor, schema.insert_pony_version_sql('0.7.*'))
            commit()
            version = '0.7.*'
        else:
            db.vdb.provider.execute(cursor, schema.get_pony_version_sql())
            version = cursor.fetchone()[0]

        ops = schema.upgrade(version, connection)

    if args['sql_only']:
        for op in ops:
            print(op.sql)
        return
    last_sql = None
    try:
        with db_session(ddl=True):
            connection = db.get_connection()
            cursor = connection.cursor()
            for op in ops:
                last_sql = op.sql
                db.vdb.provider.execute(cursor, op.sql)
                if args['verbose']:
                    print(last_sql)
            db.vdb.provider.execute(cursor, schema.set_pony_version_sql())
    except Exception as e:
        print('During applying upgrade occurred exception %r' % e, file=sys.stderr)
        if last_sql:
            print('Last SQL: %r' % last_sql)

        return


def downgrade(db, args):
    from pony.orm import db_session, core
    from pony.utils import throw
    vdb = db.vdb
    schema = vdb.schema
    with db_session:
        connection = db.get_connection()
        cursor = connection.cursor()
        res = schema.check_table_exists('pony_version', connection)
        if res is None:
            throw(core.DowngradeError, 'Downgrade can only be done from Pony version 0.9. '
                                       'Table `pony_version` was not found so we cannot detect current version')

        db.vdb.provider.execute(cursor, schema.get_pony_version_sql())
        version = cursor.fetchone()[0]
        if not version.startswith('0.9'):
            throw(core.DowngradeError, 'Downgrade can only be done from Pony version 0.9. '
                  'Your current version is %s' % version)
        ops = schema.downgrade(connection)

    if args['sql_only']:
        for op in ops:
            print(op.sql)
        return
    last_sql = None
    try:
        with db_session(ddl=True):
            connection = db.get_connection()
            cursor = connection.cursor()
            for op in ops:
                sql = op.sql
                last_sql = sql
                db.vdb.provider.execute(cursor, sql)
                if args['verbose']:
                    print(sql)
            db.vdb.provider.execute(cursor, schema.set_pony_version_sql('0.7.11'))
    except Exception as e:
        print('During applying downgrade occurred exception %r' % e, file=sys.stderr)
        if last_sql:
            print('Last SQL: %r' % last_sql)

        return
