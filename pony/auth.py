import re, os, time, sha, base64, threading, Queue

from pony.thirdparty import sqlite

ip_re = re.compile('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')

def set_user(login, ip=None):
    if '\x00' in login: raise ValueError('Login must not contains null bytes')
    if ip and not ip_re.match(ip): raise ValueError('Ivalid IP value: %s' % ip)
    minute = long(time.time()) // 60
    return _make_session(login, minute, minute, ip)
   
def get_user(session_data, ip=None, max_last=20, max_first=24*60):
    mcurrent = long(time.time()) // 60
    try:
        data = base64.b64decode(session_data)
        login, mfirst_str, mlast_str, hashcode = data.split('\x00', 3)
        mfirst = int(mfirst_str, 16)
        mlast = int(mlast_str, 16)
        if mfirst < mcurrent - max_first: return False, None, None
        if mlast < mcurrent - max_last: return False, None, None
        if mlast > mcurrent + 2: return False, None, None
        secret = get_secret(mlast)
        if secret is None: return False, None, None
        shaobject = sha.new(login)
        shaobject.update(mfirst_str)
        shaobject.update(mlast_str)
        shaobject.update(secret)
        if hashcode != shaobject.digest():
            if not ip: return False, None, None
            shaobject.update(ip)
            if hashcode != shaobject.digest(): return False, None, None
        else: ip = None
        if mlast == mcurrent: return True, login, session_data
        return True, login, _make_session(login, mfirst, mcurrent, ip)
    except:
        return False, None, None
    
def _make_session(login, mfirst, mcurrent, ip=None):
    secret = get_secret(mcurrent)
    mfirst_str = '%x' % mfirst
    mcurrent_str = '%x' % mcurrent
    shaobject = sha.new(login)
    shaobject.update(mfirst_str)
    shaobject.update(mcurrent_str)
    shaobject.update(secret)
    if ip: shaobject.update(ip)
    data = '\x00'.join([ login, mfirst_str, mcurrent_str, shaobject.digest() ])
    return base64.b64encode(data)

secret_cache = {}
queue = Queue.Queue()

class Local(threading.local):
    def __init__(self):
        self.lock = threading.Lock()
        self.lock.acquire()

local = Local()

def get_secret(minute):
    secret = secret_cache.get(minute)
    if secret is None:
        queue.put((minute, local.lock))
        local.lock.acquire()
        secret = secret_cache[minute]
    return secret

def get_sessiondb_name():
    main = sys.modules['__main__']
    try: script_name = main.__file__
    except AttributeError:  # interactive mode
        return ':memory:'   # in-memory database
    head, tail = os.path.split(script_name)
    if tail == '__init__.py': return head + '-sessions.sqlite'
    else:
        root, ext = os.path.splitext(script_name)
        return root + '-sessions.sqlite'    

sql_create = """
create table if not exists time_secrets (
    minute integer primary key,
    secret binary not null    
    );
"""

class AuthThread(threading.Thread):
    def run(self):
        con = self.connnection = sqlite.connect(get_sessiondb_name())
        con.executescript(sql_create)
        for minute, secret in con.execute('select * from time_secrets'):
            secret_cache[minute] = str(secret)
        self.connnection.commit()
        while True:
            x = queue.get()
            if x is None: break
            minute, lock = x
            if minute in secret_cache:
                lock.release()
                continue
            sql_select = 'select secret from time_secrets where minute = ?'
            row = con.execute(sql_select, [minute]).fetchone()
            if row is not None:
                con.commit()
                secret_cache[minute] = str(row[0])
                lock.release()
                continue
            current_minute = long(time.time()) // 60
            con.execute('delete from time_secrets where minute < ?',
                        [ current_minute - 24*60 ])
            secret = os.urandom(32)
            con.execute('insert into time_secrets values(?, ?)',
                        [ minute, buffer(secret) ])
            con.commit()
            secret_cache[minute] = secret
            lock.release()

auth_thread = AuthThread()
auth_thread.start()
