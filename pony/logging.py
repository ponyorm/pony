import cPickle, os, Queue, random, re, sys, traceback, thread, threading, time
from itertools import count
import pony
from pony.thirdparty import sqlite
from pony.utils import current_timestamp

process_id = os.getpid()

sql_re = re.compile('^\s*(\w+)')

def log_sql(sql, params=()):
    command = (sql_re.match(sql).group(1) or '?').upper()
    log(type='SQL:'+command, text=sql, params=params)

queue = Queue.Queue()

class Local(threading.local):
    def __init__(self):
        self.thread_id = thread.get_ident()
        self.lock = threading.Lock()
        self.lock.acquire()

local = Local()

def log(*args, **record):
    if len(args) > 0: record['type'] = args[0]
    if len(args) > 1: record['text'] = args[1]
    if len(args) > 2: assert False
    record['timestamp'] = current_timestamp()
    record['process_id'] = process_id
    record['thread_id'] = local.thread_id
    record.setdefault('trans_id', None)
    record.setdefault('type', 'unknown')
    for field in 'user', 'text':
        value = record.setdefault(field, None)
        if isinstance(value, str):
            record[field] = value.decode('utf-8', 'replace')
    queue.put(record)

log(type='Log:start')

def log_exc():
    log(type='exception',
        text=traceback.format_exception_only(*sys.exc_info()[:2])[-1][:-1],
        traceback=traceback.format_exc())

sql_query = 'select * from log where %s order by id %s limit ?'

def search_log(max_count=100, start_from=None, criteria=None, params=()):
    criteria = criteria or '1=1'
    params = list(params)
    if start_from is not None:
        params.append(start_from)
        if max_count > 0: criteria += ' and id < ?'
        else: criteria += ' and id > ?'
    direction = max_count > 0 and 'desc' or ''
    params.append(abs(max_count))
    sql = sql_query % (criteria, direction)
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
    return root + '-log.sqlite'

sql_create = """
create table if not exists log (
    id           integer primary key, -- autoincremented row id
    timestamp    timestamp not null,  
    type         text not null,       -- for example HTTP:GET or SQL:SELECT
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
id timestamp type process_id thread_id trans_id user text
'''.split()

question_marks = ', '.join(['?'] * (len(sql_columns) + 1))
sql_insert = 'insert into log values (%s)' % question_marks

class LoggerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, name="LoggerThread")
        self.setDaemon(True)
    def run(self):
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
                record = cPickle.loads(str(row[-1]))
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
            row.append(buffer(cPickle.dumps(record, 2)))
            rows.append(row)
        con = self.connection
        while True:
            try:
                con.executemany(sql_insert, rows)
                con.commit()
            except sqlite.OperationalError:
                con.rollback()
                time.sleep(random.random())
            else: break

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

@pony.on_shutdown
def do_shutdown():
    log(type='Log:shutdown')
    queue.put(None)
    logger_thread.join()

logger_thread = LoggerThread()
logger_thread.start()
