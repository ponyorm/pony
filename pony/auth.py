import re, os, time, sha, base64

session_cache = {}

ip_re = re.compile('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')

def set_user(login, ip=None):
    if '\x00' in login: raise ValueError('Login must not contains null bytes')
    if ip and not ip_re.match(ip): raise ValueError('Ivalid IP value: %s' % ip)
    minute = long(time.time()) // 60
    return _make_session(login, minute, minute, ip)
   
def get_user(session_data, ip=None, max_first = 10, max_last = 2):
    mcurrent = long(time.time()) // 60
    try:
        data = base64.b64decode(session_data)
        login, mfirst_str, mlast_str, hashcode = data.split('\x00', 3)
        mfirst = int(mfirst_str, 16)
        mlast = int(mlast_str, 16)
        if mfirst < mcurrent - max_first: return False, None, None
        if mlast < mcurrent - max_last: return False, None, None
        secret = session_cache.get(mlast)
        if secret is None: return False, None, None
        shaobject = sha.new(login)
        shaobject.update(mfirst_str)
        shaobject.update(mlast_str)
        shaobject.update(secret)
        if hashcode != shaobject.digest():
            if not ip: return None
            shaobject.update(ip)
            if hashcode != shaobject.digest(): return None
        else: ip = None
        if mlast == mcurrent: return session_data
        return True, login, _make_session(login, mfirst, mcurrent, ip)
    except:
        return False, None, None
    
def _make_session(login, mfirst, mcurrent, ip=None):
    secret = session_cache.get(mcurrent)
    if secret is None:
        secret = session_cache.setdefault(mcurrent, os.urandom(32))
    mfirst_str = '%x' % mfirst
    mcurrent_str = '%x' % mcurrent
    shaobject = sha.new(login)
    shaobject.update(mfirst_str)
    shaobject.update(mcurrent_str)
    shaobject.update(secret)
    if ip: shaobject.update(ip)
    data = '\x00'.join([ login, mfirst_str, mcurrent_str, shaobject.digest() ])
    return base64.b64encode(data)
