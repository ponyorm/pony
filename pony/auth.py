import re, os, os.path, sys, time, random, threading, Queue, cPickle, base64, hmac, sha

from urllib import quote_plus, unquote_plus

import pony
from pony.thirdparty import sqlite
from pony.utils import compress, decompress

################################################################################

def get_user():
    return local.user

def set_user(user, remember_ip=False, path='/', domain=None):
    local.set_user(user, remember_ip, path, domain)

def get_session():
    return local.session

def load(data, environ):
    local.load(data, environ)

def save(environ):
    return local.save(environ)

def get_conversation(s):
    return local.conversation

def load_conversation(s):
    local.load_conversation(s)

def save_conversation():
    return local.save_conversation()

def get_ticket(payload=None, prevent_resubmit=False):
    if not payload: payload = ''
    else:
        assert isinstance(payload, str)
        payload = compress(payload)
        
    now = int(time.time())
    now_str = '%x' % now
    rnd = os.urandom(8)
    hashobject = get_hashobject(now // 60)
    hashobject.update(rnd)
    hashobject.update(payload)
    hashobject.update(cPickle.dumps(local.user, 2))
    if prevent_resubmit: hashobject.update('+')
    hash = hashobject.digest()

    payload_str = base64.b64encode(payload)
    rnd_str = base64.b64encode(rnd)
    hash_str = base64.b64encode(hash)
    return ':'.join((now_str, payload_str, rnd_str, hash_str))

def verify_ticket(ticket_str):
    now = int(time.time() // 60)
    try:
        time_str, payload_str, rnd_str, hash_str = ticket_str.split(':')
        minute = int(time_str, 16) // 60
        if minute < now - max_mtime_diff or minute > now + 1: return False, None
        rnd = base64.b64decode(rnd_str)
        if len(rnd) != 8: return False, None
        payload = base64.b64decode(payload_str)
        hash = base64.b64decode(hash_str)
        hashobject = get_hashobject(minute)
        hashobject.update(rnd)
        hashobject.update(payload)
        hashobject.update(cPickle.dumps(local.user, 2))
        if hash != hashobject.digest():
            hashobject.update('+')
            if hash != hashobject.digest(): return False, None
            result = []
            queue.put((2, minute, buffer(rnd), local.lock, result))
            local.lock.acquire()
            if not result[0]: return result[0], None
        if payload: payload = decompress(payload)
        return (minute, rnd), payload or None
    except: return False, None

def unexpire_ticket(ticket_id):
    if not ticket_id: return
    minute, rnd = ticket_id
    queue.put((3, minute, buffer(rnd)))

################################################################################

max_ctime_diff = 24*60
max_mtime_diff = 20

class Local(threading.local):
    def __init__(self):
        self.lock = threading.Lock()
        self.lock.acquire()
        self.old_data = None
        self.session = {}
        self.user = None
        self.set_user(None)
        self.conversation = {}
    def set_user(self, user, remember_ip=False, path='/', domain=None):
        if self.user is not None or user is None: self.session.clear()
        self.user = user
        self.ctime = int(time.time() // 60)
        self.remember_ip = False
        self.path = path
        self.domain = domain
    def load(self, data, environ):
        ip = environ.get('REMOTE_ADDR', '')
        user_agent = environ.get('HTTP_USER_AGENT', '')
        self.old_data = data
        now = int(time.time() // 60)
        if data in (None, 'None'): self.set_user(None); return
        try:
            ctime_str, mtime_str, pickle_str, hash_str = data.split(':')
            self.ctime = int(ctime_str, 16)
            mtime = int(mtime_str, 16)
            if (self.ctime < now - max_ctime_diff
                  or mtime < now - max_mtime_diff
                  or mtime > now + 1
                ): self.set_user(None); return
            pickle_data = base64.b64decode(pickle_str)
            hash = base64.b64decode(hash_str)
            hashobject = get_hashobject(mtime)
            hashobject.update(ctime_str)
            hashobject.update(pickle_data)
            hashobject.update(user_agent)
            if hash != hashobject.digest():
                hashobject.update(ip)
                if hash != hashobject.digest(): self.set_user(None); return
                self.remember_ip = True
            else: self.remember_ip = False
            info = cPickle.loads(pickle_data)
            self.user, self.session, self.domain, self.path = info
        except: self.set_user(None)
    def save(self, environ):
        ip = environ.get('REMOTE_ADDR', '')
        user_agent = environ.get('HTTP_USER_AGENT', '')
        mtime = int(time.time() // 60)
        ctime_str = '%x' % self.ctime
        mtime_str = '%x' % mtime
        if self.user is None and not self.session: data = 'None'
        else:
            info = self.user, self.session, self.domain, self.path
            pickle_data = cPickle.dumps(info, 2)
            hashobject = get_hashobject(mtime)
            hashobject.update(ctime_str)
            hashobject.update(pickle_data)
            hashobject.update(user_agent)
            if self.remember_ip: hash_object.update(ip)
            pickle_str = base64.b64encode(pickle_data)
            hash_str = base64.b64encode(hashobject.digest())
            data = ':'.join([ctime_str, mtime_str, pickle_str, hash_str])
        if data == self.old_data: return None, None, None
        return data, self.domain, self.path
    def load_conversation(self, s):
        if not s:
            self.conversation = {}
            return
        try:
            s = unquote_plus(s)
            time_str, pickle_str, hash_str = s.split(':')
            minute = int(time_str, 16)
            compressed_data = base64.b64decode(pickle_str, altchars='-_')
            hash = base64.b64decode(hash_str, altchars='-_')
            hashobject = get_hashobject(minute)
            hashobject.update(compressed_data)
            if hash != hashobject.digest(): return {}
            conversation = cPickle.loads(decompress(compressed_data))
            assert conversation.__class__ == dict
            self.conversation = conversation
        except: self.conversation = {}
    def save_conversation(self):
        c = self.conversation
        if not c: return ''
        for key, value in c.items():
            class_name = key.__class__.__name__
            if class_name == 'StrHtml':
                del c[key]
                c[str.__str__(key)] = value
            elif class_name == 'Html':
                del c[key]
                c[unicode.__unicode__(key)] = value
                
        now = int(time.time() // 60)
        now_str = '%x' % now
        compressed_data = compress(cPickle.dumps(c, 2))
        hashobject = get_hashobject(now)
        hashobject.update(compressed_data)
        hash = hashobject.digest()

        pickle_str = base64.b64encode(compressed_data, altchars='-_')
        hash_str = base64.b64encode(hashobject.digest(), altchars='-_')
        s = ':'.join((now_str, pickle_str, hash_str))
        return quote_plus(s, safe=':')

local = Local()
secret_cache = {}
queue = Queue.Queue()

def get_hashobject(minute):
    hashobject = secret_cache.get(minute)
    if hashobject is None:
        queue.put((1, minute, local.lock))
        local.lock.acquire()
        hashobject = secret_cache[minute]
    return hashobject.copy()

def get_sessiondb_name():
    # This function returns relative path, if possible.
    # It is workaround for bug in SQLite
    # (Problems with unicode symbols in directory name)
    if pony.MAIN_FILE is None: return ':memory:'
    root, ext = os.path.splitext(pony.MAIN_FILE)
    if pony.RUNNED_AS == 'NATIVE': root = os.path.basename(root)
    return root + '-secrets.sqlite'

sql_create = """
create table if not exists time_secrets (
    minute integer primary key,
    secret blob not null    
    );
create table if not exists used_tickets (
    minute integer not null,
    rnd    blob  not null,
    primary key (minute, rnd)
    );
"""

class AuthThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self, name="AuthThread")
        self.setDaemon(True)
    def run(self):
        con = self.connection = sqlite.connect(get_sessiondb_name())
        try:
            con.execute("PRAGMA synchronous = OFF;")
            con.executescript(sql_create)
            for minute, secret in con.execute('select * from time_secrets'):
                secret_cache[minute] = hmac.new(str(secret), digestmod=sha)
            self.connection.commit()
            while True:
                x = queue.get()
                if x is None: break
                while True:
                    try:
                        action = x[0]
                        if action == 1: self.prepare_secret(*x[1:])
                        elif action == 2: self.check_ticket(*x[1:])
                        elif action == 3: self.unexpire_ticket(*x[1:])
                        else: assert False
                    except sqlite.OperationalError:
                        con.rollback()
                        time.sleep(random.random())
                    else: break
        finally:
            con.close()
    def prepare_secret(self, minute, lock):
        if minute in secret_cache:
            lock.release()
            return
        con = self.connection
        row = con.execute('select secret from time_secrets where minute = ?',
                          [minute]).fetchone()
        if row is not None:
            con.rollback()
            secret_cache[minute] = str(row[0])
            lock.release()
            return
        now = int(time.time() // 60)
        old = now - max_ctime_diff
        secret = os.urandom(32)
        con.execute('delete from used_tickets where minute < ?', [ old ])
        con.execute('delete from time_secrets where minute < ?', [ old ])
        con.execute('insert or ignore into time_secrets values(?, ?)',
                    [ minute, buffer(secret) ])
        con.commit()
        secret_cache[minute] = hmac.new(secret, digestmod=sha)
        lock.release()
    def check_ticket(self, minute, rnd, lock, result):
        con = self.connection
        row = con.execute('select rowid from used_tickets '
                          'where minute = ? and rnd = ?',
                          [minute, rnd]).fetchone()
        if row is None:
            con.execute('insert or ignore into used_tickets values(?, ?)', [minute, rnd])
        con.commit()
        result.append(row is None and True or None)
        lock.release()
    def unexpire_ticket(self, minute, rnd):
        con = self.connection
        con.execute('delete from used_tickets where minute = ? and rnd = ?', [minute, rnd])
        con.commit()

@pony.on_shutdown
def do_shutdown():
    queue.put(None)
    auth_thread.join()

auth_thread = AuthThread()
auth_thread.start()
