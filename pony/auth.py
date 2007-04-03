import re, os, sys, time, base64, threading, Queue

try: from hashlib import sha256 as hashfunc
except ImportError:
    from sha import new as hashfunc

from pony.thirdparty import sqlite

def create_session_id(login, user_agent='', ip=''):
    if '\x00' in login: raise ValueError('Login must not contains null bytes')
    minute = long(time.time()) // 60
    return _make_session(login, minute, minute, user_agent, ip)
   
def check_session_id(session_data, user_agent='', ip='',
                     max_last=3, max_first=24*60):
    mcurrent = long(time.time()) // 60
    login = None
    try:
        data = base64.b64decode(session_data)
        login, mfirst_str, mlast_str, hash = data.split('\x00', 3)
        mfirst = int(mfirst_str, 16)
        mlast = int(mlast_str, 16)
        if (mfirst < mcurrent - max_first or
            mlast < mcurrent - max_last or
            mlast > mcurrent + 2): return False, login, None
        hashobject = get_hashobject(mlast)
        hashobject.update(login)
        hashobject.update(mfirst_str)
        hashobject.update(user_agent)
        if hash != hashobject.digest():
            if not ip: return False, login, None
            hashobject.update(ip)
            if hash != hashobject.digest(): return False, login, None
        else: ip = ''
        if mlast == mcurrent: return True, login, session_data
        return True, login, _make_session(login, mfirst, mcurrent, ip)
    except:
        return False, login, None
    
def _make_session(login, mfirst, mcurrent, user_agent='', ip=''):
    mfirst_str = '%x' % mfirst
    mcurrent_str = '%x' % mcurrent
    hashobject = get_hashobject(mcurrent)
    hashobject.update(login)
    hashobject.update(mfirst_str)
    hashobject.update(user_agent)
    hashobject.update(ip)
    data = '\x00'.join([ login, mfirst_str, mcurrent_str, hashobject.digest() ])
    return base64.b64encode(data)

secret_cache = {}
queue = Queue.Queue()

class Local(threading.local):
    def __init__(self):
        self.lock = threading.Lock()
        self.lock.acquire()

local = Local()

def get_hashobject(minute):
    hashobject = secret_cache.get(minute)
    if hashobject is None:
        queue.put((minute, local.lock))
        local.lock.acquire()
        hashobject = secret_cache[minute]
    return hashobject.copy()

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
            secret_cache[minute] = hashfunc(str(secret))
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
            secret_cache[minute] = hashfunc(secret)
            lock.release()

auth_thread = AuthThread()
auth_thread.start()
