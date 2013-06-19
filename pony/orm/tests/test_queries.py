from __future__ import with_statement

import re, os, os.path, sys, imp

from pony import orm
from pony.orm import core
from pony.orm.tests import testutils

directive_re = re.compile(r'(\w+):')
directive = module_name = None
statements = []
lines = []

def Schema():
    if not statement_used:
        print
        print 'Statement not used:'
        print
        print '\n'.join(statements)
        print
        sys.exit()
    assert len(lines) == 1
    global module_name
    module_name = lines[0].strip()

def SQLite():
    do_test('sqlite')

def MySQL():
    do_test('mysql')

def PostgreSQL():
    do_test('postgres')

def Oracle():
    do_test('oracle')

unavailable_providers = set()

def do_test(provider_name):
    if provider_name in unavailable_providers: return
    testutils.TestDatabase.real_provider_name = provider_name
    core.Database = orm.Database = testutils.TestDatabase
    sys.modules.pop(module_name, None)
    try: __import__(module_name)
    except ImportError, e:
        print
        print 'ImportError for database provider %s:\n%s' % (provider_name, e)
        print
        unavailable_providers.add(provider_name)
        return
    module = sys.modules[module_name]
    core.debug = orm.debug = False
    globals = vars(module).copy()
    with orm.db_session:
        for statement in statements[:-1]:
            code = compile(statement, '<string>', 'exec')
            exec code in globals
        statement = statements[-1]
        try: last_code = compile(statement, '<string>', 'eval')
        except SyntaxError:
            last_code = compile(statement, '<string>', 'exec')
            exec last_code in globals
        else:
            result = eval(last_code, globals)
            if isinstance(result, core.Query): result = list(result)
        sql = module.db.sql
    expected_sql = '\n'.join(lines)
    if sql == expected_sql: print '+', provider_name, statements[-1]
    else:
        print '-', provider_name, statements[-1]
        print
        print 'Expected:'
        print expected_sql
        print
        print 'Got:'
        print sql
        print
    global statement_used
    statement_used = True

dirname, fname = os.path.split(__file__)
queries_fname = os.path.join(dirname, 'queries.txt')

def orphan_lines(lines):
    SQLite()
    lines[:] = []

statement_used = True
for raw_line in file(queries_fname):
    line = raw_line.strip()
    if not line: continue
    if line.startswith('#'): continue
    match = directive_re.match(line)
    if match:
        if directive:
            directive()
            lines[:] = []
        elif lines: orphan_lines(lines)
        directive = eval(match.group(1))
    elif line.startswith('>>> '):
        if directive:
            directive()
            lines[:] = []
            statements[:] = []
        elif lines: orphan_lines(lines)
        directive = None
        statements.append(line[4:])
        statement_used = False
    else:
        lines.append(raw_line.rstrip())

if directive:
    directive()
elif lines:
    orphan_lines(lines)
