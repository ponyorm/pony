from __future__ import absolute_import, print_function

import cPickle, os, os.path, Queue, random, re, sys, traceback, threading, time, warnings
from thread import get_ident
from itertools import count

import logging
NOTSET = logging.NOTSET
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

import pony
from pony import options
from pony.utils import current_timestamp, restore_escapes, localbase

try: process_id = os.getpid()
except AttributeError: process_id = 0 # in GAE

if pony.MODE.startswith('GAE-'): LOG_TO_SQLITE = False
elif options.LOG_TO_SQLITE is not None: LOG_TO_SQLITE = options.LOG_TO_SQLITE
else: LOG_TO_SQLITE = pony.MODE in ('CHERRYPY', 'INTERACTIVE', 'FCGI-FLUP')

logging.basicConfig(level=options.LOGGING_LEVEL or INFO, format='%(message)s')
root_logger = logging.root

pony_logger = logging.getLogger('pony')
pony_logger.setLevel(options.LOGGING_PONY_LEVEL or WARNING)

warnings_logger = logging.getLogger('warnings')
prev_showwarning = warnings.showwarning
def showwarning(message, category, filename, lineno, file=None, line=None):
    text = warnings.formatwarning(message, category, filename, lineno)
    warnings_logger.warning(text)
warnings.showwarning = showwarning

def log(*args, **record):
    if args:
        record['type'] = args[0]
        if len(args) > 1:
            record['text'] = args[1]
            assert len(args) == 2
    record.setdefault('type', 'unknown')
    for field in 'user', 'text':
        value = record.get(field)
        # if isinstance(value, str): record[field] = value.decode('utf-8', 'replace')
    level = record.get('severity') or INFO
    type = record.get('type', '')
    if pony_logger.level <= level and root_logger.level <= level and not type.startswith('logging:'):
        prefix = record.get('prefix', '')
        text = record.get('text', '')
        if type != 'exception': message = prefix + text
        else:
            message = record['traceback']
            if text.startswith('SyntaxError: '):
                message = message.decode('utf-8', 'replace').encode('cp1251')
        message = restore_escapes(message)
        if message: pony_logger.log(level, message)
    if LOG_TO_SQLITE:
        record['timestamp'] = current_timestamp()
        record['process_id'] = process_id
        record['thread_id'] = local.thread_id
        record.setdefault('trans_id', None)
        queue.put(record) # record can be modified inside LoggerThread

def log_exc():
    log(type='exception', prefix='Exception: ', severity=WARNING,
        text=traceback.format_exception_only(*sys.exc_info()[:2])[-1][:-1], traceback=traceback.format_exc())

sql_re = re.compile('^\s*(\w+)')

def log_sql(sql, params=()):
    command = (sql_re.match(sql).group(1) or '?').upper()
    log(type='SQL:'+command, prefix='SQL: ', text=sql, severity=DEBUG, params=params)

hdr_list = '''
    ACTUAL_SERVER_PROTOCOL
    AUTH_TYPE
    HTTP_ACCEPT
    HTTP_ACCEPT_CHARSET
    HTTP_ACCEPT_ENCODING
    HTTP_ACCEPT_LANGUAGE
    HTTP_CONNECTION
    HTTP_COOKIE
    HTTP_HOST
    HTTP_KEEP_ALIVE
    HTTP_USER_AGENT
    PATH_INFO
    QUERY_STRING
    REMOTE_ADDR
    REMOTE_PORT
    REQUEST_METHOD
    SCRIPT_NAME
    SERVER_NAME
    SERVER_PORT
    SERVER_PROTOCOL
    SERVER_SOFTWARE
    wsgi.url_scheme
    '''.split()

hdr_dict1 = dict((header, i) for i, header in enumerate(hdr_list))
hdr_dict2 = dict(enumerate(hdr_list))

def compress_record(record):
    type = record['type']
    if type.startswith('HTTP:') and type[5].isupper():
        get = hdr_dict1.get
        headers = dict((get(header, header), value) for (header, value) in record['headers'].items())
        record['headers'] = headers

def decompress_record(record):
    type = record['type']
    if type.startswith('HTTP:') and type[5].isupper():
        get = hdr_dict2.get
        headers = dict((get(header, header), value) for (header, value) in record['headers'].items())
        record['headers'] = headers

if LOG_TO_SQLITE:
    class PonyHandler(logging.Handler):
        def emit(handler, record):
            if record.name == 'pony': return
            if record.exc_info:
                if not record.exc_text: record.exc_text = logging._defaultFormatter.formatException(record.exc_info)
                kwargs = {'exc_text': record.exc_text}
            else: kwargs = {}
            log(type='logging:%s' % record.name, text=record.getMessage(),
                severity=record.levelno, module=record.module, lineno=record.lineno, **kwargs)
    pony_handler = PonyHandler()
    logging.root.addHandler(pony_handler)

    queue = Queue.Queue()

    class Local(localbase):
        def __init__(local):
            local.thread_id = get_ident()
            local.lock = threading.Lock()
            local.lock.acquire()

    local = Local()

    def search_log(max_count=100, start_from=None, criteria=None, params=()):
        criteria = criteria or '1=1'
        params = list(params)
        if start_from is not None:
            params.append(start_from)
            if max_count > 0: criteria += ' and id < ?'
            else: criteria += ' and id > ?'
        direction = 'desc' if max_count > 0 else ''
        params.append(abs(max_count))
        sql = 'select * from log where %s order by id %s limit ?' % (criteria, direction)
        result = []
        queue.put((sql, params, result, local.lock))
        local.lock.acquire()
        if result and isinstance(result[0], Exception): raise result[0]
        return result

    def get_logfile_name():
        # This function returns relative path, if possible.
        # It is workaround for bug in SQLite
        # (Problems with unicode symbols in directory name)
        if pony.MAIN_FILE is None: return ':memory:'
        root, ext = os.path.splitext(pony.MAIN_FILE)
        if pony.MODE != 'MOD_WSGI': root = os.path.basename(root)
        return root + '-log.sqlite'

    sql_create = """
    create table if not exists log (
        id           integer primary key, -- autoincremented row id
        timestamp    timestamp not null,
        type         text not null,       -- for example HTTP:GET or SQL:SELECT
        severity     integer,
        process_id   integer  not null,
        thread_id    integer  not null,
        trans_id     integer,             -- reserved for future use; must be NULL
        user         text,                -- current user login
        text         text,                -- url, sql query, debug message, etc.
        pickle_data  binary               -- all other data in pickled form
        );
    create index if not exists index_log_timestamp on log (timestamp, type);
    """

    sql_columns = '''
    id timestamp type severity process_id thread_id trans_id user text
    '''.split()

    question_marks = ', '.join(['?'] * (len(sql_columns) + 1))
    sql_insert = 'insert into log values (%s)' % question_marks

    class LoggerThread(threading.Thread):
        def __init__(thread):
            threading.Thread.__init__(thread, name="LoggerThread")
            thread.setDaemon(True)
        def run(thread):
            import sqlite3 as sqlite
            global OperationalError
            OperationalError = sqlite.OperationalError
            con = thread.connection = sqlite.connect(get_logfile_name())
            try:
                con.execute("PRAGMA synchronous = OFF;")
                con.executescript(sql_create)
                con.commit()
                while True:
                    x = queue.get()
                    if x is None: break
                    elif not isinstance(x, dict): thread.execute_query(*x)
                    else:
                        records = [ x ]
                        while True:
                            try: x = queue.get_nowait()
                            except Queue.Empty: break
                            if not isinstance(x, dict): break
                            records.append(x)
                        thread.save_records(records)
                        if x is None: break
                        elif not isinstance(x, dict): thread.execute_query(*x)
            finally:
                con.close()
        def execute_query(thread, sql, params, result, lock):
            try:
                try: cursor = thread.connection.execute(sql, params)
                except Exception as e:
                    result.append(e); return
                for row in cursor:
                    record = cPickle.loads(str(row[-1]).decode('zip'))
                    for i, name in enumerate(sql_columns): record[name] = row[i]
                    decompress_record(record)
                    result.append(record)
            finally:
                lock.release()
                thread.connection.rollback()
        def save_records(thread, records):
            rows = []
            for record in records:
                record.pop('prefix', None)
                compress_record(record)
                row = [ record.pop(name, None) for name in sql_columns ]
                row.append(buffer(cPickle.dumps(record, 2).encode('zip')))
                rows.append(row)
            con = thread.connection
            while True:
                try:
                    con.executemany(sql_insert, rows)
                    con.commit()
                except OperationalError:
                    con.rollback()
                    time.sleep(random.random())
                else: break

    @pony.on_shutdown
    def do_shutdown():
        log(type='Log:shutdown', severity=INFO)
        queue.put(None)
        logger_thread.join()

    logger_thread = LoggerThread()
    logger_thread.start()
    log(type='Log:start', severity=INFO)
