from __future__ import print_function
from pony.py23compat import basestring

import os, os.path, sys, shlex
from datetime import datetime
from glob import glob

from docopt import docopt

import pony
from pony import orm

from . import writer, get_cmd_exitstack, get_migration_dir
from .exceptions import MigrationCommandError, MigrationFileNotFound, MultipleMigrationFilesFound, MergeAborted
from .migration import Migration, MigrationGraph, make_migration_entity
from .questioner import InteractiveMigrationQuestioner

CLI_DOC = '''
Pony migration tool.

Usage:
    {script_name} make [<name>] [--verbose | -v] [--empty --custom]
    {script_name} apply [[<start>] <end>] [--verbose | -v] [--fake-initial --dry]
    {script_name} sql <name>
    {script_name} list

Commands:
    make          Generate the migration file
    apply         Apply all generated migrations
    merge         Merge conflicts
    list          List all migrations
    sql           View sql for a given migration

Options:
    --empty       Generate a template for data migration
    --fake-initial  Fake the first migration
    --dry         Just print sql instead without executing it
    -v --verbose  Set sql_debug(True)
    -h --help     Show this screen
'''

cmd_exitstack = get_cmd_exitstack()


class drop_into_debugger(object):
    def __enter__(self):
        pass
    def __exit__(self, e, m, tb):
        if not e:
            return
        try:
            import ipdb as pdb
        except ImportError:
            import pdb
        print(m.__repr__(), file=sys.stderr)
        pdb.post_mortem(tb)


def migrate(db, argv=None):
    cmd, kwargs = parse_migrate_options(argv)
    return _migrate(db, cmd, **kwargs)


migrate_options = dict(
    name='<name>', start='<start>', end='<end>',
    verbose='--verbose', custom='--custom', dry='--dry', empty='--empty', fake_initial='--fake-initial'
)


def parse_migrate_options(argv):
    if isinstance(argv, basestring):
        argv = shlex.split(argv)
    doc = CLI_DOC.format(script_name='migrate')
    opts = docopt(doc, argv)
    cmd_list=[cmd for cmd in ('make', 'apply', 'list', 'sql') if opts[cmd]]
    assert len(cmd_list) == 1
    cmd = cmd_list[0]
    kwargs = {kw: opts[opt] for kw, opt in migrate_options.items()}
    if kwargs['start'] and not kwargs['end']:
        # https://github.com/docopt/docopt/issues/358
        kwargs['end'], kwargs['start'] = kwargs['start'], kwargs['end']
    return cmd, kwargs


def _migrate(db, cmd, name=None, start=None, end=None,
            verbose=False, custom=False, dry=False, empty=False, fake_initial=False):
    debug = os.environ.get('PONY_DEBUG')
    if debug:
        cmd_exitstack.enter_context(drop_into_debugger())
    if verbose:
        orm.sql_debug(True)

    graph = MigrationGraph()
    with cmd_exitstack:
        if cmd == 'make':
            return graph.make(db=db, empty=empty, custom=custom, description=name)
        elif cmd == 'list':
            show_migrations(db=db)
        elif cmd == 'apply':
            start = find_migration(start) if start else None
            end = find_migration(end) if end else None
            graph.apply(db=db, is_fake=fake_initial, dry_run=dry, name_start=start, name_end=end)
        elif cmd == 'sql':
            name = find_migration(name)
            graph.apply(db=db, dry_run=True, name_exact=name)
        else:
            raise MigrationCommandError('%s is not a valid migration subcommand' % cmd)

def find_migration(prefix):
    template = prefix + '*.py'
    pathname = os.path.join(get_migration_dir(), template)
    files = glob(pathname)
    if not files:
        raise MigrationFileNotFound('No files for {}'.format(prefix))
    elif len(files) > 1:
        files = ', '.join(os.path.basename(filename) for filename in files)
        raise MultipleMigrationFilesFound('Multiple files found: {}'.format(files))
    return os.path.basename(files[0])[:-3]

@orm.db_session
def show_migrations(db, fail_fast=False):
    '''
    List the migration dir.
    if migration name is specified, print its sql.
    '''
    cmd_exitstack.callback(db.disconnect)
    make_migration_entity(db)
    db.schema = db.generate_schema()
    graph = MigrationGraph()
    leaves = graph.leaves()
    if not leaves:
        print('No migrations')
        return
    if len(leaves) > 1 and not fail_fast:
        # Merge required
        questioner = InteractiveMigrationQuestioner()
        if questioner.ask_merge(leaves):
            # not tested?
            merge(graph=graph, leaves=leaves)
            show_migrations(fail_fast=True)
            return
        return
    leaf = leaves[0]
    names = leaf.forwards_list()

    try:
        with orm.db_session:
            orm.exists(m for m in db.Migration)
    except orm.core.DatabaseError as ex:
        print('No Migration table. Please apply the initial migration.')
        return

    saved = orm.select((m.name, m.applied) for m in db.Migration if m.name in names).order_by(2)[:]
    if saved:
        saved, _ = zip(*saved)
    for name in saved:
        print('+ {}'.format(name))
    for name in names:
        if name in saved:
            continue
        print('  {}'.format(name))

def merge(db=None, graph=None, leaves=None):
    if graph is None:
        graph = MigrationGraph()
        leaves = graph.leaves()
    if len(leaves) <= 1:
        print('Nothing to merge.')
        return

    questioner = InteractiveMigrationQuestioner()
    if not questioner.ask_merge(leaves):
        raise MergeAborted

    cmd_exitstack.callback(db.disconnect)
    name = graph.make_next_migration_name(description='merge', with_timestamp=True)
    ctx = {
        'deps': leaves,
        'version': pony.__version__,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M"),
        'body': 'operations = []',
        'imports': '',
    }
    generated = writer.MIGRATION_TEMPLATE.format(**ctx)
    migrations = get_migration_dir()
    p = os.path.join(migrations, '{}.py'.format(name))
    with open(p, 'w') as f:
        f.write(generated)


def add_migrate_to_click(click_group, db, name='migrate'):
    '''
    Compatibility function for click (click.pocoo.org).

    For flask app:
        add_migrate_to_click(app.cli, db)
    '''
    import click

    @click.command(name, context_settings={'ignore_unknown_options': True})
    @click.argument('_arg', nargs=-1, type=click.UNPROCESSED)
    def do_migrate(_arg):
        migrate(db)

    click_group.add_command(do_migrate)
