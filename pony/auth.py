import re, os, os.path, sys, time, random, threading, Queue, cPickle, base64, hmac, sha
from binascii import hexlify
from urllib import quote_plus, unquote_plus

import pony
from pony.utils import compress, decompress

import pony.sessionstorage.ramstorage as storage

MAX_CTIME_DIFF = 24*60
MAX_MTIME_DIFF = 30

def get_user():
    return local.user

def set_user(user, remember_ip=False, path='/', domain=None):
    local.set_user(user, remember_ip, path, domain)

def get_session():
    return local.session

def load(data, environ):
    ip = environ.get('REMOTE_ADDR', '')
    user_agent = environ.get('HTTP_USER_AGENT', '')
    local.old_data = data
    now = int(time.time() // 60)
    if data in (None, 'None'): set_user(None); return
    try:
        ctime_str, mtime_str, pickle_str, hash_str = data.split(':')
        local.ctime = int(ctime_str, 16)
        mtime = int(mtime_str, 16)
        if (local.ctime < now - MAX_CTIME_DIFF
              or mtime < now - MAX_MTIME_DIFF
              or mtime > now + 1
            ): set_user(None); return
        pickle_data = base64.b64decode(pickle_str)
        hash = base64.b64decode(hash_str)
        hashobject = _get_hashobject(mtime)
        hashobject.update(ctime_str)
        hashobject.update(pickle_data)
        hashobject.update(user_agent)
        if hash != hashobject.digest():
            hashobject.update(ip)
            if hash != hashobject.digest(): set_user(None); return
            local.remember_ip = True
        else: local.remember_ip = False
        if pickle_data.startswith('A'): pickle_data = pickle_data[1:]
        elif pickle_data.startswith('B'):
            pickle_data = storage.getdata(pickle_data[1:], local.ctime, mtime)
        else: set_user(None); return
        info = cPickle.loads(decompress(pickle_data))
        local.user, local.session, local.domain, local.path = info
    except: set_user(None)

def save(environ):
    ip = environ.get('REMOTE_ADDR', '')
    user_agent = environ.get('HTTP_USER_AGENT', '')
    now = int(time.time() // 60)
    ctime_str = '%x' % local.ctime
    mtime_str = '%x' % now
    if local.user is None and not local.session: data = 'None'
    else:
        info = local.user, local.session, local.domain, local.path
        pickle_data = compress(cPickle.dumps(info, 2))
        if len(pickle_data) <= 10: # <= 4000
            pickle_data = 'A' + pickle_data
        else: pickle_data = 'B' + storage.putdata(pickle_data, local.ctime, now)
        hashobject = _get_hashobject(now)
        hashobject.update(ctime_str)
        hashobject.update(pickle_data)
        hashobject.update(user_agent)
        if local.remember_ip: hash_object.update(ip)
        pickle_str = base64.b64encode(pickle_data)
        hash_str = base64.b64encode(hashobject.digest())
        data = ':'.join([ctime_str, mtime_str, pickle_str, hash_str])
    if data == local.old_data: return None, None, None
    return data, local.domain, local.path

def get_conversation(s):
    return local.conversation

def load_conversation(s):
    if not s: local.conversation = {}; return
    try:
        s = unquote_plus(s)
        time_str, pickle_str, hash_str = s.split(':')
        minute = int(time_str, 16)
        now = int(time.time() // 60)
        if minute < now - MAX_MTIME_DIFF or minute > now + 1:
            local.conversation = {}; return
        
        compressed_data = base64.b64decode(pickle_str, altchars='-_')
        hash = base64.b64decode(hash_str, altchars='-_')
        hashobject = _get_hashobject(minute)
        hashobject.update(compressed_data)
        if hash != hashobject.digest():
            local.conversation = {}; return
        conversation = cPickle.loads(decompress(compressed_data))
        assert conversation.__class__ == dict
        local.conversation = conversation
    except: local.conversation = {}

def save_conversation():
    c = local.conversation
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
    hashobject = _get_hashobject(now)
    hashobject.update(compressed_data)
    hash = hashobject.digest()

    pickle_str = base64.b64encode(compressed_data, altchars='-_')
    hash_str = base64.b64encode(hashobject.digest(), altchars='-_')
    s = ':'.join((now_str, pickle_str, hash_str))
    return quote_plus(s, safe=':')

def get_ticket(payload=None, prevent_resubmit=False):
    if not payload: payload = ''
    else:
        assert isinstance(payload, str)
        payload = compress(payload)
        
    now = int(time.time())
    now_str = '%x' % now
    rnd = os.urandom(8)
    hashobject = _get_hashobject(now // 60)
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
        if minute < now - MAX_MTIME_DIFF or minute > now + 1: return False, None
        rnd = base64.b64decode(rnd_str)
        if len(rnd) != 8: return False, None
        payload = base64.b64decode(payload_str)
        hash = base64.b64decode(hash_str)
        hashobject = _get_hashobject(minute)
        hashobject.update(rnd)
        hashobject.update(payload)
        hashobject.update(cPickle.dumps(local.user, 2))
        if hash != hashobject.digest():
            hashobject.update('+')
            if hash != hashobject.digest(): return False, None
            result = _verify_ticket(minute, rnd)
            if not result: return result, None
        if payload: payload = decompress(payload)
        return (minute, rnd), payload or None
    except: return False, None

def unexpire_ticket(ticket_id):
    if not ticket_id: return
    minute, rnd = ticket_id
    _unexpire_ticket(minute, rnd)
    

################################################################################

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

local = Local()
secret_cache = {}

if not pony.RUNNED_AS.startswith('GAE-'):

    queue = Queue.Queue()

    def _verify_ticket(minute, rnd):
        result = []
        queue.put((2, minute, buffer(rnd), local.lock, result))
        local.lock.acquire()
        return result[0]

    def _unexpire_ticket(minute, rnd):
        queue.put((3, minute, buffer(rnd)))

    def _get_hashobject(minute):
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
            from pony.thirdparty import sqlite
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
                            if action == 1: self._get_hashobject(*x[1:])
                            elif action == 2: self._verify_ticket(*x[1:])
                            elif action == 3: self._unexpire_ticket(*x[1:])
                            else: assert False
                        except sqlite.OperationalError:
                            con.rollback()
                            time.sleep(random.random())
                        else: break
            finally:
                con.close()
        def _get_hashobject(self, minute, lock):
            if minute in secret_cache:
                lock.release()
                return
            con = self.connection
            row = con.execute('select secret from time_secrets where minute = ?', [minute]).fetchone()
            if row is not None:
                con.rollback()
                secret_cache[minute] = str(row[0])
                lock.release()
                return
            now = int(time.time() // 60)
            old = now - MAX_CTIME_DIFF
            secret = os.urandom(32)
            con.execute('delete from used_tickets where minute < ?', [ old ])
            con.execute('delete from time_secrets where minute < ?', [ old ])
            con.execute('insert or ignore into time_secrets values(?, ?)', [ minute, buffer(secret) ])
            row = con.execute('select secret from time_secrets where minute = ?', [minute]).fetchone()
            con.commit()
            secret = str(row[0])
            secret_cache[minute] = hmac.new(secret, digestmod=sha)
            lock.release()
        def _verify_ticket(self, minute, rnd, lock, result):
            con = self.connection
            row = con.execute('select rowid from used_tickets where minute = ? and rnd = ?', [minute, rnd]).fetchone()
            if row is None: con.execute('insert or ignore into used_tickets values(?, ?)', [minute, rnd])
            con.commit()
            result.append(row is None and True or None)
            lock.release()
        def _unexpire_ticket(self, minute, rnd):
            con = self.connection
            con.execute('delete from used_tickets where minute = ? and rnd = ?', [minute, rnd])
            con.commit()

    @pony.on_shutdown
    def do_shutdown():
        queue.put(None)
        auth_thread.join()

    auth_thread = AuthThread()
    auth_thread.start()

else:
    from google.appengine.ext import db
    from google.appengine.api import users

    class PonyTimeSecrets(db.Model):
        minute = db.IntegerProperty(required=True)
        secret = db.BlobProperty(required=True)

    class PonyUsedTickets(db.Model):
        minute = db.IntegerProperty(required=True)
        rnd = db.BlobProperty(required=True)

    for time_secret in PonyTimeSecrets.all():
        secret_cache[time_secret.minute] = hmac.new(time_secret.secret, digestmod=sha)

    def _verify_ticket(minute, rnd):
        keystr = 'm%s_%s' % (minute, hexlify(rnd))
        ticket = PonyUsedTickets.get_by_key_name(keystr)
        if ticket is None:
            while True:
                try: PonyUsedTickets(key_name=keystr, minute=minute, rnd=rnd).put()
                except db.TransactionFailedError: pass
                else: break
                if PonyUsedTickets.get_by_key_name(keystr) is not None: break
        return not ticket and True or None

    def _unexpire_ticket(minute, rnd):
        keystr = 'm%s_%s' % (minute, hexlify(rnd))
        ticket = PonyUsedTickets.get_by_key_name([keystr])
        if not ticket: return
        try: db.delete(ticket)
        except db.TransactionFailedError: pass

    def _get_hashobject(minute):
        hashobject = secret_cache.get(minute)
        if hashobject is not None: return hashobject.copy()

        keystr = 'm%s' % minute
        secretobj = PonyTimeSecrets.get_by_key_name(keystr)
        if secretobj is None:
            now = int(time.time() // 60)
            old = now - MAX_CTIME_DIFF
            secret = os.urandom(32)
            for ticket in PonyUsedTickets.gql('where minute < :1', minute):
                try: db.delete(ticket)
                except db.TransactionFailedError: pass
            for secretobj in PonyTimeSecrets.gql('where minute < :1', minute):
                try: db.delete(secretobj)
                except db.TransactionFailedError: pass
            while True:
                try: secretobj = PonyTimeSecrets.get_or_insert(keystr, minute=minute, secret=secret)
                except db.TransactionFailedError: continue
                else: break
        hashobject = hmac.new(secretobj.secret, digestmod=sha)
        secret_cache[minute] = hashobject
        return hashobject.copy()
