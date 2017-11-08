from pony.py23compat import ExitStack

import os

MIGRATIONS_DIR = 'migrations'

def get_migration_dir():
    return os.path.abspath(
        os.environ.get('MIGRATIONS_DIR', MIGRATIONS_DIR)
    )

def get_cmd_exitstack(_stack=ExitStack()):
    return _stack
