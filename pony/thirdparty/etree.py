try: from lxml.etree import *
except ImportError:
    try: from xml.etree.ElementTree import *
    except ImportError:
        try: from cElementTree import *
        except ImportError:
            from elementtree.ElementTree import *
