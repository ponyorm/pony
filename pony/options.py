DEBUG = True

STATIC_DIR = None

CUT_TRACEBACK = True

#postprocessing options:
STD_DOCTYPE = '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">'
STD_STYLESHEETS = [
    ("/pony/static/blueprint/screen.css", "screen, projection"),
    ("/pony/static/blueprint/print.css", "print"),
    ("/pony/static/blueprint/ie.css.css", "screen, projection", "if IE"),
    ("/pony/static/css/default.css", "screen, projection"),
    ]
BASE_STYLESHEETS_PLACEHOLDER = '<!--PONY-BASE-STYLESHEETS-->'
COMPONENT_STYLESHEETS_PLACEHOLDER = '<!--PONY-COMPONENTS-STYLESHEETS-->'
SCRIPTS_PLACEHOLDER = '<!--PONY-SCRIPTS-->'

# reloading options:
RELOADING_CHECK_INTERVAL = 1.0  # in seconds

# logging options:
LOG_TO_SQLITE = None
LOGGING_LEVEL = None
LOGGING_PONY_LEVEL = None

#auth options:
MAX_SESSION_CTIME = 60*24  # one day
MAX_SESSION_MTIME = 60*2  # 2 hours
MAX_LONGLIFE_SESSION = 14  # 14 days
COOKIE_SERIALIZATION_TYPE = 'json' # may be 'json' or 'pickle'
COOKIE_NAME = 'pony'
COOKIE_PATH = '/'
COOKIE_DOMAIN = None
HASH_ALGORITHM = None  # sha-1 by default
# HASH_ALGORITHM = hashlib.sha512

SESSION_STORAGE = None  # pony.sessionstorage.memcachedstorage by default
# SESSION_STORAGE = mystoragemodule
# SESSION_STORAGE = False  # means use cookies for save session data,
                           # can lead to race conditions

# memcached options (ignored under GAE):
MEMCACHE = None  # Use in-process python version by default
# MEMCACHE = [ "127.0.0.1:11211" ]
# MEMCACHE = MyMemcacheConnectionImplementation(...)
ALTERNATIVE_SESSION_MEMCACHE = None     # Use general memcache connection by default
ALTERNATIVE_ORM_MEMCACHE = None         # Use general memcache connection by default
ALTERNATIVE_TEMPLATING_MEMCACHE = None  # Use general memcache connection by default
ALTERNATIVE_RESPONCE_MEMCACHE = None    # Use general memcache connection by default

# pickle options:
PICKLE_START_OFFSET = 230
PICKLE_HTML_AS_PLAIN_STR = True

# encoding options for pony.pathces.repr
RESTORE_ESCAPES = True
SOURCE_ENCODING = None
CONSOLE_ENCODING = None

# db options
MAX_FETCH_COUNT = None

# used for select(...).show()
CONSOLE_WIDTH = 80

# sql translator options
SIMPLE_ALIASES = True  # if True just use entity name like "Course-1"
                       # if False use attribute names chain as an alias like "student-grades-course"

INNER_JOIN_SYNTAX = False # put conditions to INNER JOIN ... ON ... or to WHERE ...

# debugging options
DEBUGGING_REMOVE_ADDR = True
DEBUGGING_RESTORE_ESCAPES = True
