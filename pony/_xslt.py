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

##block_level_tags = set('''
##    address blockquote center dir div dl fieldset form h1 h2 h3 h4 h5 h6 hr
##    isindex menu noframes noscript ol p pre table ul'''.split())

layout_tags = set('header footer sidebar content column'.split())
layout_tags_xpath = etree.XPath('|'.join(layout_tags))

def normalize(xml):
    head = xml.find('head')
    body = xml.find('body')
    if head is not None and layout_tags_xpath(head): pass
    elif body is None: return
    elif layout_tags_xpath(body): pass
    else:
        for p in body.findall('p'):
            if layout_tags_xpath(p): break
        else: return
    if body is None:
        body = xml.makeelement('body')
        xml.append('body')
    head_list = make_head_list(head)
    body_list = make_body_list(body)
    content = column = None
    for x in head_list + body_list:
        if isinstance(x, basestring):
            assert not column
            if content is None or explicit_columns:
                content = SubElement(body, 'content')
                explicit_columns = False
            column = SubElement(content, 'column')
            column.text = x
        else:
            tag = x.tag
            if tag == 'column':
                if content is None or not explicit_columns:
                    content = SubElement(body, 'content')
                    explicit_columns = True
                column = None
                content.append(x)
            elif tag in layout_tags:
                if tag == 'content': normalize_content(x)
                content = column = None
                body.append(x)
            else:
                if column is None or explicit_columns:
                    if content is None or explicit_columns:
                        content = SubElement(body, 'content')
                        explicit_columns = False
                    column = SubElement(content, 'column')
                column.append(x)

def make_head_list(head):
    if head is None: return []
    result = []
    elements = layout_tags_xpath(head)
    for x in elements: head.remove(x)
    head_list = []
    for tag, group in groupby(elements, attrgetter('tag')):
        if tag == 'column': 
            content = head.makeelement('content')
            content.extend(group)
            head_list.append(content)
        else: head_list.extend(group)
    return head_list

def make_body_list(body):
    result = []
    append = result.append
    text = body.text
    if text and (not text.isspace() or u'\xa0' in text):
        body.text = None
        append(text)
    for element in body:
        tag = element.tag
        if tag == 'p' or tag in layout_tags:
            tail = element.tail
            if tail and (not tail.isspace() or u'\xa0' in tail):
                element.tail = None
                append(element)
                append(tail)
            else: append(element)
        else: append(element)
    body[:] = []
    return correct_p_elements(result)

def correct_p_elements(body_list):
    result = []
    for x in body_list:
        result.append(x)
        if getattr(x, 'tag', None) == 'p':
            for i, y in enumerate(x):
                if y.tag in layout_tags:
                    result.extend(correct_p_element(x, i))
                    break
    return result

def correct_p_element(p, index):
    tmp = p[index:]
    result = []
    append = result.append
    for element in tmp:
        tail = element.tail
        if tail and (not tail.isspace() or u'\xa0' in tail):
            element.tail = None
            append(element)
            append(tail)
        else: append(element)
    p[index:] = []
    return result

def normalize_content(content):
    columns = []
    text = content.text
    column = None
    if text and (not text.isspace() or u'\xa0' in text):
        column = content.makeelement('column')
        columns.append(column)
        column.text = text
        content.text = None
    for x in content:
        if x.tag == 'column':
            column = None
            columns.append(x)
            tail = x.tail
            if tail and (not tail.isspace() or u'\xa0' in tail):
                column = content.makeelement('column')
                columns.append(column)
                column.text = tail
                x.tail = None
        else:
            if column is None:
                column = content.makeelement('column')
                columns.append(column)
            column.append(x)
    content[:] = columns

xslt_filename = os.path.join(os.path.dirname(__file__), 'transform.xslt')
xslt = etree.XSLT(etree.parse(xslt_filename))

def transform(xml):
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
