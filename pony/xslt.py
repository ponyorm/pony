from pony.thirdparty import etree

is_supported = hasattr(etree, 'XSLT')

def html2xml(x, encoding='ascii'):
    if hasattr(x, 'wirite_c14n'): return x
    if not isinstance(x, basestring):
        if hasattr(x, '__unicode__'): x = unicode(x)
        else: x = str(x)
    if isinstance(x, str): x = unicode(x, encoding)
    return etree.HTML(x)

def transform(xml):
    return xml

xml2html_template = """
<xsl:stylesheet version = '1.0'
                xmlns:xsl='http://www.w3.org/1999/XSL/Transform'>
    <xsl:output method="html" encoding="%s" indent="%s" %s/>
    <xsl:template match="*">
        <xsl:copy-of select="*" />
    </xsl:template>
</xsl:stylesheet>
"""

doctypes = dict(
    strict=['-//W3C//DTD HTML 4.01//EN',
            'http://www.w3.org/TR/html4/strict.dtd'],
    transitional=['-//W3C//DTD HTML 4.01 Transitional//EN',
                  'http://www.w3.org/TR/1999/REC-html401-19991224/loose.dtd'],
    frameset=['-//W3C//DTD HTML 4.01 Frameset//EN',
              'http://www.w3.org/TR/1999/REC-html401-19991224/frameset.dtd'],
    )
doctypes['loose'] = doctypes['transitional']

xml2html_cache = {}

def xml2html(xml, encoding='UTF-8', doctype='transitional', indent=False):
    encoding = encoding.upper().replace('_', '-')
    xslt = xml2html_cache.get((encoding, doctype, indent))
    if xslt is None:
        if len(doctype) == 2: pyblic, system = doctype
        else: public, system = doctypes.get(doctype, ['', ''])
        if public: public = 'doctype-public="%s"' % public
        if system: system = 'doctype-system="%s"' % system
        text = xml2html_template % (
            encoding, indent and 'yes' or 'no', ' '.join((public, system)))
        xslt = etree.XSLT(etree.XML(text))
        xml2html_cache[(encoding, doctype, indent)] = xslt
    result = xslt(xml)
    result = str(result)
    return result
