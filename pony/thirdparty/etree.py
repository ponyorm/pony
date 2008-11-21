try: from lxml.etree import *
except ImportError:
    try: from xml.etree.cElementTree import *
    except ImportError:
        try: from cElementTree import *
        except ImportError:
            from elementtree.ElementTree import *
        else: from elementtree.ElementTree import _namespace_map
    else: from xml.etree.ElementTree import _namespace_map
