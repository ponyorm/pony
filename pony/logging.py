import cPickle, os, os.path, Queue, random, re, sys, traceback, thread, threading, time, warnings
from itertools import count

from python import logging

import pony
from pony import options
from pony.utils import current_timestamp

try: process_id = os.getpid()
except AttributeError: # in GAE
    process_id = 0

if pony.MODE.startswith('GAE-'): log_to_sqlite = False
elif options.log_to_sqlite is not None: log_to_sqlite = options.log_to_sqlite
else: log_to_sqlite = pony.MODE in ('CHERRYPY', 'INTERACTIVE')

verbose = log_to_sqlite

format = '[%(type)s] %(text)s'

def log(*args, **record):
    if len(args) > 0: record['type'] = args[0]
    if len(args) > 1: record['text'] = args[1]
    if len(args) > 2: assert False
    record.setdefault('type', 'unknown')
    for field in 'user', 'text':
        value = record.setdefault(field, None)
        if isinstance(value, str): record[field] = value.decode('utf-8', 'replace')
    if log_to_sqlite:
        record['timestamp'] = current_timestamp()
        record['process_id'] = process_id
        record['thread_id'] = local.thread_id
        record.setdefault('trans_id', None)
        queue.put(record)
    else:
        type = record['type']
        level = record.get('severity')
        if level is not None: pass
        elif 'traceback' in record: level = logging.ERROR
        else: level = logging.INFO
        pony_logger.log(level, format, record)

def log_exc():
    log(type='exception',
        text=traceback.format_exception_only(*sys.exc_info()[:2])[-1][:-1],
        traceback=traceback.format_exc())

sql_re = re.compile('^\s*(\w+)')

def log_sql(sql, params=()):
    command = (sql_re.match(sql).group(1) or '?').upper()
    log(type='SQL:'+command, text=sql, params=params)

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
        headers = dict((get(header, header), value)
                       for (header, value) in record['headers'].items())
        record['headers'] = headers

def decompress_record(record):
    type = record['type']
    if type.startswith('HTTP:') and type[5].isupper():
        get = hdr_dict2.get
        headers = dict((get(header, header), value)
                       for (header, value) in record['headers'].items())
        record['headers'] = headers

if options.logging_base_level is not None: base_lebel = options.logging_base_level
elif pony.MODE == 'MOD_WSGI': base_level = logging.WARNING
else: base_level = logging.INFO

if not log_to_sqlite:
    logging.basicConfig(level=base_level)
    pony_logger = logging.getLogger('pony')
else:
    prev_showwarning = warnings.showwarning
    def showwarning(message, category, filename, lineno):
        log(type='warning', text=str(message), category=category.__name__, filename=filename, lineno=lineno)
        prev_showwarning(message, category, filename, lineno)
    warnings.showwarning = showwarning

    class PonyHandler(logging.Handler):
        def emit(self, record):
            if record.exc_info:
                if not record.exc_text: record.exc_text = logging._defaultFormatter.formatException(record.exc_info)
                keyargs = {'exc_text': record.exc_text}
            else: keyargs = {}
            log(type='logging:%s' % record.levelname, text=record.getMessage(),
                severity=record.levelno, module=record.module, lineno=record.lineno, **keyargs)
    if not logging.root.handlers:
        logging.root.addHandler(PonyHandler())
        logging.root.setLevel(logging.INFO)

    queue = Queue.Queue()

    class Local(threading.local):
        def __init__(self):
            self.thread_id = thread.get_ident()
            self.lock = threading.Lock()
            self.lock.acquire()

    local = Local()

    def search_log(max_count=100, start_from=None, criteria=None, params=()):
        criteria = criteria or '1=1'
        params = list(params)
        if start_from is not None:
            params.append(start_from)
            if max_count > 0: criteria += ' and id < ?'
            else: criteria += ' and id > ?'
        direction = max_count > 0 and 'desc' or ''
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
        if pony.MODE == 'CHERRYPY': root = os.path.basename(root)
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
        def __init__(self):
            threading.Thread.__init__(self, name="LoggerThread")
            self.setDaemon(True)
        def run(self):
            from pony.thirdparty import sqlite
            global OperationalError
            OperationalError = sqlite.OperationalError
            con = self.connection = sqlite.connect(get_logfile_name())
            try:
                con.execute("PRAGMA synchronous = OFF;")
                con.executescript(sql_create)
                con.commit()
                while True:
                    x = queue.get()
                    if x is None: break
                    elif not isinstance(x, dict): self.execute_query(*x)
                    else:
                        records = [ x ]
                        while True:
                            try: x = queue.get_nowait()
                            except Queue.Empty: break
                            if not isinstance(x, dict): break
                            records.append(x)
                        self.save_records(records)
                        if x is None: break
                        elif not isinstance(x, dict): self.execute_query(*x)
            finally:
                con.close()
        def execute_query(self, sql, params, result, lock):
            try:
                try:
                    cursor = self.connection.execute(sql, params)
                except Exception, e:
                    result.append(e)
                    return
                for row in cursor:
                    record = cPickle.loads(str(row[-1]).decode('zip'))
                    for i, name in enumerate(sql_columns): record[name] = row[i]
                    decompress_record(record)
                    result.append(record)
            finally:
                lock.release()
                self.connection.rollback()
        def save_records(self, records):
            rows = []
            for record in records:
                compress_record(record)
                row = [ record.pop(name, None) for name in sql_columns ]
                row.append(buffer(cPickle.dumps(record, 2).encode('zip')))
                rows.append(row)
            con = self.connection
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
        log(type='Log:shutdown')
        queue.put(None)
        logger_thread.join()

    logger_thread = LoggerThread()
    logger_thread.start()
    log(type='Log:start')
