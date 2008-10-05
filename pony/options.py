STD_DOCTYPE = '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">'
STD_STYLESHEETS = [
    ("/pony/static/blueprint/screen.css", "screen, projection"),
    ("/pony/static/blueprint/print.css", "print"),
    ("/pony/static/blueprint/ie.css.css", "screen, projection", "if IE"),
    ("/pony/static/blueprint/default.css", "screen, projection"),
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

# encoding options for pony.pathces.repr
SOURCE_ENCODING = None
CONSOLE_ENCODING = None

# db options
MAX_ROWS_COUNT = 1000