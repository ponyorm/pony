import re, os, os.path, sys, threading, hmac, sha
from base64 import b64encode, b64decode
from binascii import hexlify
from cPickle import loads, dumps
from random import random
from time import time, sleep
from urllib import quote_plus, unquote_plus

import pony
from pony import options, webutils
from pony.utils import compress, decompress, simple_decorator
from pony.sessionstorage import ramstorage as storage

class Local(threading.local):
    def __init__(self):
        self.lock = threading.Lock()
        self.lock.acquire()
        self.clear()
    def clear(self):
        now = int(time()) // 60
        lock = self.lock
        self.__dict__.clear()
        self.__dict__.update(lock=lock, user=None, environ={}, session={}, conversation={}, ctime=now, mtime=now,
                             cookie_value=None, remember_ip=False,
                             ip=None, user_agent=None, ticket=False, ticket_payload=None)
    def set_user(self, user, remember_ip=False):
        if self.user is not None or user is None:
            self.session.clear()
            self.conversation.clear()
        self.user = user
        self.remember_ip = remember_ip

local = Local()

secret_cache = {}

set_user = local.set_user

def get_user():
    return local.user

def get_session():
    return local.session

def get_hashobject(minute):
    hashobject = secret_cache.get(minute) or _get_hashobject(minute)
    return hashobject.copy()

def load(environ, cookies=None):
    local.clear()
    local.environ = environ
    if cookies is None:
        cookies =  Cookie.SimpleCookie()
        if 'HTTP_COOKIE' in environ: cookies.load(environ['HTTP_COOKIE'])
    morsel = cookies.get(options.COOKIE_NAME)
    local.cookie_value = cookie_value = morsel and morsel.value or None
    if not cookie_value: return
    now = int(time()) // 60
    local.ip = ip = environ.get('REMOTE_ADDR')
    local.user_agent = user_agent = environ.get('HTTP_USER_AGENT')
    try:
        ctime_str, mtime_str, pickle_str, hash_str = cookie_value.split(':')
        ctime = local.ctime = int(ctime_str, 16)
        mtime = local.mtime = int(mtime_str, 16)
        ctime_diff = now - ctime
        mtime_diff = now - mtime
        if ctime_diff < -1 or mtime_diff < -1: return
        if ctime_diff > options.MAX_SESSION_CTIME or mtime_diff > options.MAX_SESSION_MTIME:
            return
        pickle_data = b64decode(pickle_str)
        hash = b64decode(hash_str)
        hashobject = get_hashobject(mtime)
        hashobject.update(ctime_str)
        hashobject.update(pickle_data)
        hashobject.update(user_agent or '')
        if hash != hashobject.digest():
            hashobject.update(ip or '')
            if hash != hashobject.digest(): return
            local.remember_ip = True
        else: local.remember_ip = False
        if pickle_data.startswith('A'):
            compressed_data = pickle_data[1:]
        elif pickle_data.startswith('B'):
            compressed_data = storage.getdata(pickle_data[1:], ctime, mtime)
        else: return
        info = loads(decompress(compressed_data))
        local.user, local.session = info
    except: return

def save(cookies):
    now = int(time()) // 60
    ctime_str = '%x' % local.ctime
    mtime_str = '%x' % now
    if local.user is None and not local.session: cookie_value = ''
    else:
        info = local.user, local.session
        compressed_data = compress(dumps(info, 2))
        if len(compressed_data) <= 10: # <= 4000
            pickle_data = 'A' + compressed_data
        else: pickle_data = 'B' + storage.putdata(compressed_data, local.ctime, now)
        hashobject = get_hashobject(now)
        hashobject.update(ctime_str)
        hashobject.update(pickle_data)
        hashobject.update(local.user_agent or '')
        if local.remember_ip: hashobject.update(local.ip or '')
        pickle_str = b64encode(pickle_data)
        hash_str = b64encode(hashobject.digest())
        cookie_value = ':'.join([ctime_str, mtime_str, pickle_str, hash_str])
    if cookie_value == local.cookie_value: return None
    webutils.set_cookie(cookies, options.COOKIE_NAME, cookie_value, options.COOKIE_EXPIRES, options.COOKIE_MAX_AGE,
                        options.COOKIE_PATH, options.COOKIE_DOMAIN, http_only=True)

def get_conversation(s):
    return local.conversation

def load_conversation(fields):
    s = fields.getfirst(options.CONVERSATION_FIELD_NAME)
    if not s: return
    try:
        s = unquote_plus(s)
        time_str, pickle_str, hash_str = s.split(':')
        minute = int(time_str, 16)
        now = int(time()) // 60
        if minute < now - options.MAX_SESSION_MTIME or minute > now + 1: return
        compressed_data = b64decode(pickle_str, altchars='-_')
        hash = b64decode(hash_str, altchars='-_')
        hashobject = get_hashobject(minute)
        hashobject.update(compressed_data)
        if hash != hashobject.digest(): return
        conversation = loads(decompress(compressed_data))
        assert conversation.__class__ == dict
        local.conversation = conversation
    except: return

def save_conversation():
    c = local.conversation
    if not c: return ''
    now = int(time()) // 60
    now_str = '%x' % now
    compressed_data = compress(dumps(c, 2))
    hashobject = get_hashobject(now)
    hashobject.update(compressed_data)
    hash = hashobject.digest()

    pickle_str = b64encode(compressed_data, altchars='-_')
    hash_str = b64encode(hashobject.digest(), altchars='-_')
    s = ':'.join((now_str, pickle_str, hash_str))
    return quote_plus(s, safe=':')

def get_ticket(payload=None, prevent_resubmit=False):
    if not payload: payload = ''
    else:
        assert isinstance(payload, str)
        payload = compress(payload)
        
    now = int(time()) // 60
    now_str = '%x' % now
    rnd = os.urandom(8)
    hashobject = get_hashobject(now)
    hashobject.update(rnd)
    hashobject.update(payload)
    hashobject.update(dumps(local.user, 2))
    if prevent_resubmit: hashobject.update('+')
    hash = hashobject.digest()

    payload_str = b64encode(payload)
    rnd_str = b64encode(rnd)
    hash_str = b64encode(hash)
    return ':'.join((now_str, payload_str, rnd_str, hash_str))

def verify_ticket(ticket_str):
    now = int(time()) // 60
    try:
        time_str, payload_str, rnd_str, hash_str = ticket_str.split(':')
        minute = int(time_str, 16)
        if minute < now - options.MAX_SESSION_MTIME or minute > now + 1: return
        rnd = b64decode(rnd_str)
        if len(rnd) != 8: return
        payload = b64decode(payload_str)
        hash = b64decode(hash_str)
        hashobject = get_hashobject(minute)
        hashobject.update(rnd)
        hashobject.update(payload)
        hashobject.update(dumps(local.user, 2))
        if hash != hashobject.digest():
            hashobject.update('+')
            if hash != hashobject.digest(): return
            result = _verify_ticket(minute, rnd)
            if not result: local.ticket = result; return
        if payload: payload = decompress(payload)
        local.ticket = minute, rnd
        local.ticket_payload = payload or None
    except: return

def unexpire_ticket():
    if not local.ticket: return
    minute, rnd = local.ticket
    _unexpire_ticket(minute, rnd)
    
if not pony.MODE.startswith('GAE-'):

    from Queue import Queue
    queue = Queue()

    @simple_decorator
    def exec_in_auth_thread(f, *args, **keyargs):
        result_holder = []
        queue.put((local.lock, f, args, keyargs, result_holder))
        local.lock.acquire()
        return result_holder[0]

    @simple_decorator
    def exec_async(f, *args, **keyargs):
        queue.put((None, f, args, keyargs, None))

    connection = None

    @exec_in_auth_thread
    def _verify_ticket(minute, rnd):
        rnd = buffer(rnd)
        row = connection.execute('select rowid from used_tickets where minute = ? and rnd = ?', [minute, rnd]).fetchone()
        if row is None: connection.execute('insert or ignore into used_tickets values(?, ?)', [minute, rnd])
        connection.commit()
        return row is None and True or None

    @exec_async
    def _unexpire_ticket(minute, rnd):
        connection.execute('delete from used_tickets where minute = ? and rnd = ?', [minute, buffer(rnd)])
        connection.commit()

    @exec_in_auth_thread
    def _get_hashobject(minute):
        result = secret_cache.get(minute)
        if result: return result
        row = connection.execute('select secret from time_secrets where minute = ?', [minute]).fetchone()
        if row is None:
            now = int(time()) // 60
            secret = os.urandom(32)
            connection.execute('delete from used_tickets where minute < ?', [ now - options.MAX_SESSION_MTIME ])
            connection.execute('delete from time_secrets where minute < ?', [ now - options.MAX_SESSION_MTIME ])
            connection.execute('insert or ignore into time_secrets values(?, ?)', [ minute, buffer(secret) ])
            row = connection.execute('select secret from time_secrets where minute = ?', [minute]).fetchone()
            connection.commit()
        else: connection.rollback()
        secret = str(row[0])
        secret_cache[minute] = result = hmac.new(secret, digestmod=sha)
        return result

    def get_sessiondb_name():
        # This function returns relative path, if possible.
        # It is workaround for bug in SQLite
        # (Problems with unicode symbols in directory name)
        if pony.MAIN_FILE is None: return ':memory:'
        root, ext = os.path.splitext(pony.MAIN_FILE)
        if pony.MODE == 'CHERRYPY': root = os.path.basename(root)
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
            global connection
            connection = sqlite.connect(get_sessiondb_name())
            try:
                connection.execute("PRAGMA synchronous = OFF;")
                connection.executescript(sql_create)
                for minute, secret in connection.execute('select * from time_secrets'):
                    secret_cache[minute] = hmac.new(str(secret), digestmod=sha)
                connection.commit()
                while True:
                    x = queue.get()
                    if x is None: break
                    lock, func, args, keyargs, result_holder = x
                    while True:
                        try: result = func(*args, **keyargs)
                        except sqlite.OperationalError:
                            connection.rollback()
                            sleep(random())
                        else: break
                    if result_holder is not None: result_holder.append(result)
                    if lock is not None: lock.release()
            finally: connection.close()

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
            now = int(time()) // 60
            old = now - options.MAX_SESSION_MTIME
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
