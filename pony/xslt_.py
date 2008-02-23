import threading

from lxml import etree

##xslt_filename = os.path.join(pony.MAIN_DIR, 'transform.xslt')
##xslt_transformer = etree.XSLT(etree.parse(xslt_filename))

def transform(html, charset):
    xml = html2xml(html, charset)
    normalize(xml)
    # xml = xslt_transformer(xml)
    return xml2html(xml, charset)

def normalize(html):
    head = html.find('head')
    body = html.find('body')
    if body is None: body = SubElement(html, 'body')
    if head is None:
        head = html.makeelement('head')
        html.insert(-1, head)

def html2xml(x, encoding='ascii'):
    if hasattr(x, 'write_c14n'): return x
    if not isinstance(x, basestring):
        if hasattr(x, '__unicode__'): x = unicode(x)
        else: x = str(x)
    if isinstance(x, str): x = unicode(x, encoding)
    return etree.HTML(x)

if etree.LIBXML_VERSION >= (2, 6, 28) and etree.LIBXSLT_VERSION >= (1, 1, 19):
    xml2html_template = """
    <xsl:stylesheet version = '1.0' xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>
      <xsl:output method="html" encoding="%s" indent="%s" %s/>
      <xsl:template match="/">
        <xsl:copy-of select="." />
      </xsl:template>
    </xsl:stylesheet>
    """
else: xml2html_template = """
    <xsl:stylesheet version = '1.0'
                    xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>
      <xsl:output method="html" encoding="%s" indent="%s" %s/>
      <xsl:template match="/ | @* | node()">
        <xsl:copy>
          <xsl:apply-templates select="@* | node()" />
        </xsl:copy>
      </xsl:template>
    </xsl:stylesheet>
    """

doctypes = dict(
    strict=['-//W3C//DTD HTML 4.01//EN', 'http://www.w3.org/TR/html4/strict.dtd'],
    transitional=['-//W3C//DTD HTML 4.01 Transitional//EN', 'http://www.w3.org/TR/1999/REC-html401-19991224/loose.dtd'],
    frameset=['-//W3C//DTD HTML 4.01 Frameset//EN', 'http://www.w3.org/TR/1999/REC-html401-19991224/frameset.dtd'],
    )
doctypes['loose'] = doctypes['transitional']

class Local(threading.local):
    def __init__(self):
        self.xml2html_cache = {}

local = Local()

def create_xml2html_transformer(encoding, doctype, indent):
    if len(doctype) == 2: pyblic, system = doctype
    else: public, system = doctypes.get(doctype, ['', ''])
    if public: public = 'doctype-public="%s"' % public
    if system: system = 'doctype-system="%s"' % system
    text = xml2html_template % (encoding, indent and 'yes' or 'no', ' '.join((public, system)))
    xml = etree.XML(text)
    xslt = etree.XSLT(xml)
    return xslt

std_encoding = 'UTF-8'
std_doctype = 'transitional'
std_indent = True

main_xml2html_transformer = create_xml2html_transformer(std_encoding, std_doctype, std_indent)

def xml2html(xml, encoding='UTF-8', doctype='transitional', indent=True):
    encoding = encoding.upper().replace('_', '-')
    if encoding != std_encoding or doctype != std_doctype or indent != std_indent:
        xslt = local.xml2html_cache.get((encoding, doctype, indent))
        if xslt is None:
            xslt = create_xml2html_transformer(encoding, doctype, indent)
            local.xml2html_cache[(encoding, doctype, indent)] = xslt
    else: xslt = main_xml2html_transformer
    result = xslt(xml)
    return (str(result).replace('<!--{{if IE}}-->', '<!--[if IE]>')
                       .replace('\n<!--{{endif}}-->', '<![endif]-->\n')
                       .replace('<!--{{endif}}-->', '<![endif]-->\n'))
