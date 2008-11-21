try: from lxml.etree import *
except ImportError:
    try: from xml.etree.cElementTree import *
    except ImportError:
        try: from cElementTree import *
        except ImportError:
            from elementtree.ElementTree import *
