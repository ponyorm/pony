'''
Usage:
  editor --export <path_to_db> --file=<out_file>
  editor --upload <path_to_db> [--login=<login> --password=<password>] <name>
'''

# FIXME
URL = 'http://localhost:5001/import_project'

from docopt import docopt
import json
import requests

from .util import resolve_name
from .diagram import db_to_diagram

import os
from pony.py23compat import PY2
if PY2:
    from contextlib2 import ExitStack
else:
    from contextlib import ExitStack

with ExitStack() as stack:
    if os.environ.get('DEBUG'):
        import ipdb
        stack.enter_context(ipdb.launch_ipdb_on_exception())

    opts = docopt(__doc__)
    if opts['--export']:
        raise NotImplementedError
    
    name = opts['<name>']
    path = opts['<path_to_db>']
    db = resolve_name(path)
    diagram = db_to_diagram(db)
    
    login = opts['--login']
    password = opts['--password']
    if not (login and password):
        print('Please enter your credentials at editor.ponyorm.com')
        login = input('Login: ')
        passwd = input('Password: ')
    resp = requests.post(URL, json={
        'login': login,
        'password': passwd,
        'diagram': json.dumps(diagram),
        'diagram_type': 'pony',
        'diagram_name': name,
        'private': True,
    })
    resp = json.loads(resp.text)
    print('\nYour diagram is available at http://editor.ponyorm.com%s' % resp["link"])