try: import lxml
except ImportError: is_supported = False
else:
    is_supported = True
    from xslt_ import *
