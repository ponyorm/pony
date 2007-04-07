import re, os, sys, time, base64, threading, Queue, cPickle, Cookie

try: from hashlib import sha256 as hashfunc
except ImportError:
    from sha import new as hashfunc

from pony.thirdparty import sqlite

################################################################################

def get_user():
    return local.user

def set_user(user, remember_ip=False):
    local.set_user(user, remember_ip)

def get_session():
    return local.session

def load(data, ip=''):
    local.load(data, ip)

def save(ip=''):
    return local.save(ip)

################################################################################

use_http_only = True

max_ctime_diff = 24*60
max_mtime_diff = 20
max_mtime_future_diff = 20

class Local(threading.local):
    def __init__(self):
        self.lock = threading.Lock()
        self.lock.acquire()
        self.old_data = None
        self.set_user(None)
    def set_user(self, user, remember_ip=False):
        self.user = user
        self.ctime = int(time.time() // 60)
        self.remember_ip = False
        self.session = {}
    def load(self, data, ip=''):
        self.old_data = data
        now = int(time.time() // 60)
        if data in (None, 'None'): self.set_user(None); return
        try:
            ctime_str, mtime_str, pickle_str, hash_str = data.split(':')
            self.ctime = int(ctime_str, 16)
            mtime = int(mtime_str, 16)
            if (self.ctime < now - max_ctime_diff
                  or mtime < now - max_mtime_diff
                  or mtime > now + max_mtime_future_diff
                ): self.set_user(None); return
            pickle_data = base64.b64decode(pickle_str)
            hash = base64.b64decode(hash_str)

            hashobject = get_hashobject(mtime)
            hashobject.update(ctime_str)
            hashobject.update(pickle_data)
            if hash != hashobject.digest():
                hashobject.update(ip)
                if hash != hashobject.digest(): self.set_user(None); return
                self.remember_ip = True
            else: self.remember_ip = False
            self.user, self.session = cPickle.loads(pickle_data)
        except: self.set_user(None)
    def save(self, ip=''):
        ctime = self.ctime
        mtime = int(time.time() // 60)
        ctime_str = '%x' % ctime
        mtime_str = '%x' % mtime
        if self.user is None: data = 'None'
        else:
            pickle_data = cPickle.dumps((self.user, self.session), 2)
            hashobject = get_hashobject(mtime)
            hashobject.update(ctime_str)
            hashobject.update(pickle_data)
            if self.remember_ip: hash_object.update(ip)

            pickle_str = base64.b64encode(pickle_data)
            hash_str = base64.b64encode(hashobject.digest())
            data = ':'.join([ctime_str, mtime_str, pickle_str, hash_str])
        if data == self.old_data: return None
        return data

local = Local()
secret_cache = {}
queue = Queue.Queue()

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
    if tail == '__init__.py': return head + '-secrets.sqlite'
    else:
        root, ext = os.path.splitext(script_name)
        return root + '-secrets.sqlite'    

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
