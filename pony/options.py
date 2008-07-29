# logging options:
LOG_TO_SQLITE = None
LOGGING_LEVEL = None
LOGGING_PONY_LEVEL = None

#auth options:
MAX_COOKIE_SIZE = 3000
MAX_SESSION_CTIME = 60*24  # one day
MAX_SESSION_MTIME = 60*2  # 2 hours
MAX_LONGLIFE_SESSION = 14  # 14 days
COOKIE_SERIALIZATION_TYPE = 'json' # may be 'json' or 'pickle'
COOKIE_NAME = 'pony'
COOKIE_PATH = '/'
COOKIE_DOMAIN = None
HASH_ALGORITHM = None  # None means sha-1
CONVERSATION_FIELD_NAME = '_c'

# pickle options:
PICKLE_START_OFFSET = 230
PICKLE_HTML_AS_PLAIN_STR = True
