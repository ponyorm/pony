try: import lxml
except ImportError:
    is_supported = False
    def xslt_function(f): pass
else:
    is_supported = True
    from xslt_ import *
