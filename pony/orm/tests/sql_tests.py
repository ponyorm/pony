from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2

import re, os, os.path, sys
from datetime import datetime, timedelta

from pony import orm
from pony.orm import core
from pony.orm.tests import testutils

core.suppress_debug_change = True

directive_re = re.compile(r'(\w+)(\s+[0-9\.]+)?:')
directive = module_name = None
statements = []
lines = []

def Schema(param):
    if not statement_used:
        print()
        print('Statement not used:')
        print()
        print('\n'.join(statements))
        print()
        sys.exit()
    assert len(lines) == 1
    global module_name
    module_name = lines[0].strip()

def SQLite(server_version):
    do_test('sqlite', server_version)

def MySQL(server_version):
    do_test('mysql', server_version)

def PostgreSQL(server_version):
    do_test('postgres', server_version)

def Oracle(server_version):
    do_test('oracle', server_version)

unavailable_providers = set()

def do_test(provider_name, raw_server_version):
    if provider_name in unavailable_providers: return
    testutils.TestDatabase.real_provider_name = provider_name
    testutils.TestDatabase.raw_server_version = raw_server_version
    core.Database = orm.Database = testutils.TestDatabase
    sys.modules.pop(module_name, None)
    try: __import__(module_name)
    except ImportError as e:
        print()
        print('ImportError for database provider %s:\n%s' % (provider_name, e))
        print()
        unavailable_providers.add(provider_name)
        return
    module = sys.modules[module_name]
    globals = vars(module).copy()
    globals.update(datetime=datetime, timedelta=timedelta)
    with orm.db_session:
        for statement in statements[:-1]:
            code = compile(statement, '<string>', 'exec')
            if PY2:
                exec('exec code in globals')
            else:
                exec(code, globals)
        statement = statements[-1]
        try: last_code = compile(statement, '<string>', 'eval')
        except SyntaxError:
            last_code = compile(statement, '<string>', 'exec')
            if PY2:
                exec('exec last_code in globals')
            else:
                exec(last_code, globals)
        else:
            result = eval(last_code, globals)
            if isinstance(result, core.Query): result = list(result)
        sql = module.db.sql
    expected_sql = '\n'.join(lines)
    if sql == expected_sql: print('.', end='')
    else:
        print()
        print(provider_name, statements[-1])
        print()
        print('Expected:')
        print(expected_sql)
        print()
        print('Got:')
        print(sql)
        print()
    global statement_used
    statement_used = True

dirname, fname = os.path.split(__file__)
queries_fname = os.path.join(dirname, 'queries.txt')

def orphan_lines(lines):
    SQLite(None)
    lines[:] = []

statement_used = True
for raw_line in open(queries_fname):
    line = raw_line.strip()
    if not line: continue
    if line.startswith('#'): continue
    match = directive_re.match(line)
    if match:
        if directive:
            directive(directive_param)
            lines[:] = []
        elif lines: orphan_lines(lines)
        directive = eval(match.group(1))
        if match.group(2):
            directive_param = match.group(2)
        else: directive_param = None
    elif line.startswith('>>> '):
        if directive:
            directive(directive_param)
            lines[:] = []
            statements[:] = []
        elif lines: orphan_lines(lines)
        directive = None
        directive_param = None
        statements.append(line[4:])
        statement_used = False
    else:
        lines.append(raw_line.rstrip())

if directive:
    directive(directive_param)
elif lines:
    orphan_lines(lines)
