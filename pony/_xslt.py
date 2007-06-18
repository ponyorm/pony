import os.path
from operator import attrgetter
from itertools import groupby

from lxml import etree
from lxml.etree import SubElement

def html2xml(x, encoding='ascii'):
    if hasattr(x, 'wirite_c14n'): return x
    if not isinstance(x, basestring):
        if hasattr(x, '__unicode__'): x = unicode(x)
        else: x = str(x)
    if isinstance(x, str): x = unicode(x, encoding)
    return etree.HTML(x)

block_level_tags = set('''
    address blockquote center dir div dl fieldset form h1 h2 h3 h4 h5 h6 hr
    isindex menu noframes noscript ol p pre table ul'''.split())

layout_tags = set('header footer sidebar content column layout'.split())
layout_tags_xpath = etree.XPath('|'.join(layout_tags))

def normalization_is_needed(html):
    head = html.find('head')
    body = html.find('body')
    if head is not None and layout_tags_xpath(head): return True
    elif body is None: return False
    elif layout_tags_xpath(body): return True
    else:
        for p in body.findall('p'):
            if layout_tags_xpath(p): return True
    return False

def normalize(html):
    if not normalization_is_needed(html): return
    head = html.find('head')
    body = html.find('body')
    body_list = []
    if head is not None: body_list.extend(layout_tags_xpath(head))
    if body is None: body = SubElement(html, 'body')
    else:
        text = body.text
        if text and (not text.isspace() or u'\xa0' in text):
            body_list.append(text)
            body.text = None
        body_list.extend(unnest(body))
    body[:] = normalize_elements(html, body_list)

def unnest(elements):
    result = []
    for x in elements:
        result.append(x)
        tag, tail = x.tag, x.tail
        if tag == 'layout':
            result.extend(unnest(x))
            x[:] = []
        elif tag == 'p' or tag in layout_tags:
            for i, y in enumerate(x):
                ytag = y.tag
                if y.tag in layout_tags:
                    if tag == 'content' and ytag == 'column': continue
                    result.extend(unnest(x[i:]))
                    x[i:] = []
                    break
        if tail and not tail.isspace():
            result.append(x.tail)
            x.tail = None
    return result

def normalize_elements(document, elements):
    result = []
    content = column = None
    explicit_columns = False
    for x in elements:
        tag = getattr(x, 'tag', None)
        if tag == 'column':
            if content is None or not explicit_columns:
                if content is not None: normalize_content(content)
                content = document.makeelement('content')
                result.append(content)
                explicit_columns = True
            column = None
            content.append(x)
        elif tag in layout_tags:
            if content is not None: normalize_content(content)
            if tag == 'content': normalize_content(x)
            elif tag in ('layout', 'sidebar'): normalize_width(x)
            content = column = None
            result.append(x)
        else:
            if column is None or explicit_columns:
                if content is None or explicit_columns:
                    if content is not None: normalize_content(content)
                    content = document.makeelement('content')
                    result.append(content)
                    explicit_columns = False
                column = SubElement(content, 'column')
            if tag is not None: column.append(x)
            elif not len(column): column.text = (column.text or '') + x
            else:
                 last_child = column[-1]
                 last_child.tail = (last_child.tail or '') + x
    if content is not None: normalize_content(content)
    return result

def normalize_content(content):
    list = []; append = list.append
    column = None
    text = content.text
    if text and not text.isspace():
        column = content.makeelement('column')
        append(column)
        column.text = text
        content.text = None
    for x in content:
        if x.tag == 'column':
            if column is not None: normalize_column(column)
            normalize_column(x)
            column = None
            append(x)
            tail = x.tail
            if tail and not tail.isspace():
                column = content.makeelement('column')
                append(column)
                column.text = tail
                x.tail = None
        else:
            if column is None:
                column = content.makeelement('column')
                append(column)
            column.append(x)
    if column is not None: normalize_column(column)
    content[:] = list

def normalize_column(column):
    list = []; append = list.append
    p = None
    text = column.text
    if text and not text.isspace():
        p = column.makeelement('p')
        append(p)
        p.text = text
        column.text = None
    for x in column:
        if x.tag in block_level_tags:
            p = None
            append(x)
            tail = x.tail
            if tail and not tail.isspace():
                p = column.makeelement('p')
                append(p)
                p.text = tail
                x.tail = None
        else:
            if p is None:
                p = column.makeelement('p')
                append(p)
            p.append(x)
    column[:] = list

def normalize_width(element):
    width = element.get('width')
    if width is None or width[-2:] != 'px': return
    try: number = int(width[:-2])
    except ValueError: return
    element.set('width', width[:-2])
    
xslt_filename = os.path.join(os.path.dirname(__file__), 'transform.xslt')
xslt = etree.XSLT(etree.parse(xslt_filename))

def transform(xml):
    f = file('c:\\test\\page.xml', 'w')
    xml.getroottree().write(f)
    f.close()
    return xslt(xml)

xml2html_template = """
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
    strict=['-//W3C//DTD HTML 4.01//EN',
            'http://www.w3.org/TR/html4/strict.dtd'],
    transitional=['-//W3C//DTD HTML 4.01 Transitional//EN',
                  'http://www.w3.org/TR/1999/REC-html401-19991224/loose.dtd'],
    frameset=['-//W3C//DTD HTML 4.01 Frameset//EN',
              'http://www.w3.org/TR/1999/REC-html401-19991224/frameset.dtd'],
    )
doctypes['loose'] = doctypes['transitional']

xml2html_cache = {}

def xml2html(xml, encoding='UTF-8', doctype='transitional', indent=True):
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
    return (str(result).replace('<!--start of IE hack-->', '<!--[if IE]>')
                       .replace('\n<!--end of IE hack-->', '<![endif]-->\n')
                       .replace('<!--end of IE hack-->', '<![endif]-->\n'))
