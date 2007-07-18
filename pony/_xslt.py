import os.path
from operator import attrgetter
from itertools import groupby

from lxml import etree
from lxml.etree import SubElement

def html2xml(x, encoding='ascii'):
    if hasattr(x, 'write_c14n'): return x
    if not isinstance(x, basestring):
        if hasattr(x, '__unicode__'): x = unicode(x)
        else: x = str(x)
    if isinstance(x, str): x = unicode(x, encoding)
    return etree.HTML(x)

def normalize(html):
    # print etree.tostring(html, pretty_print=True), '\n'
    head = html.find('head')
    body = html.find('body')
    if body is None: body = SubElement(html, 'body')
    if head is None:
        head = html.makeelement('head')
        html.insert(-1, head)
    head_list = layout_tags_xpath(head)
    body[0:0] = head_list
    if head_list: pass
    elif layout_tags_xpath(body): pass
    else:
        for p in body.findall('p'):
            if layout_tags_xpath(p): break
        else: return
    i = 0
    while i < len(body):
        x = body[i]
        tag = x.tag
        if tag == 'p': unnest_p(x)
        elif tag in layout_tags:
            if tag == 'layout': unnest_layout(x)
            elif tag == 'content': unnest_content(x)
            else: unnest_element(x)
        i += 1
    # print etree.tostring(html, pretty_print=True), '\n'
    elements = []
    content = None
    text = body.text
    if text and not text.isspace():
        content = body.makeelement('content')
        content.text = text
        body.text = None
        elements.append(content)
    for x in body:
        tag = x.tag
        if tag in layout_tags and tag != 'column':
            if content is not None: normalize_content(content)
            content = None
            elements.append(x)
            tail = x.tail
            layout_tags[tag](x)
            if tail and not tail.isspace():
                content = body.makeelement('content')
                content.text = tail
                x.tail = None
                elements.append(content)
        else:
            if content is None:
                content = body.makeelement('content')
                elements.append(content)
            content.append(x)
    if content is not None: normalize_content(content)
    body[:] = elements
    # print etree.tostring(html, pretty_print=True), '\n'

def normalize_layout(layout):
    normalize_width(layout)

def normalize_header(header):
    pass

def normalize_footer(footer):
    pass

def normalize_sidebar(sidebar):
    normalize_width(sidebar)

def normalize_content(content):
    elements = []
    column = None
    text = content.text
    if text and not text.isspace():
        column = content.makeelement('column')
        column.text = text
        content.text = None
        elements.append(column)
    for x in content:
        if x.tag == 'column':
            if column is not None: normalize_column(column)
            column = None
            elements.append(x)
            tail = x.tail
            normalize_column(x)
            if tail and not tail.isspace():
                column = content.makeelement('column')
                column.text = tail
                x.tail = None
                elements.append(column)
        else:
            if column is None:
                column = content.makeelement('column')
                elements.append(column)
            column.append(x)
    if column is not None: normalize_column(column)
    content[:] = elements
    width_list = correct_width_list([column.get('width') for column in content])
    for column, width in zip(content, width_list):
        column.set('width', width or '')
    content.set('pattern', '-'.join(width or '?' for width in width_list))

yui_columns = {
    2 : [ ('1/2', '1/2'),
          ('1/3', '2/3'), ('2/3', '1/3'),
          ('1/4', '3/4'), ('3/4', '1/4') ],
    3 : [ ('1/3', '1/3', '1/3'),
          ('1/2', '1/4', '1/4'),
          ('1/4', '1/4', '1/2') ],
    4 : [ ('1/4', '1/4', '1/4', '1/4') ]
    }

def correct_width_list(width_list):
    lists = yui_columns.get(len(width_list))
    if lists is None: return width_list
    for list in lists:
        for w1, w2 in zip(width_list, list):
            if w1 and w1 != w2: break
        else: return list
    return width_list

def normalize_column(column):
    elements = []
    p = None
    text = column.text
    if text and not text.isspace():
        p = column.makeelement('p')
        p.text = text
        column.text = None
        elements.append(p)
    for x in column:
        if x.tag in block_level_tags:
            p = None
            elements.append(x)
            tail = x.tail
            if tail and not tail.isspace():
                p = column.makeelement('p')
                p.text = tail
                x.tail = None
                elements.append(p)
        else:
            if p is None:
                p = column.makeelement('p')
                elements.append(p)
            p.append(x)
    column[:] = elements

def normalize_width(element):
    width = element.get('width')
    if width is None or width[-2:] != 'px': return
    try: number = int(width[:-2])
    except ValueError: return
    element.set('width', width[:-2])

layout_tags = dict(
    layout=normalize_layout,
    header=normalize_header,
    footer=normalize_footer,
    sidebar=normalize_sidebar,
    content=normalize_content,
    column=normalize_column,
    )
layout_tags_xpath = etree.XPath('|'.join(layout_tags))

block_level_tags = set('''
    address blockquote center dir div dl fieldset form h1 h2 h3 h4 h5 h6 hr
    isindex menu noframes noscript ol p pre table ul'''.split())

def _unnest(x):
    parent = x.getparent()
    tail = parent.tail
    if tail and not tail.isspace():
        last = parent[-1]
        last.tail = (last.tail or '') + tail
        parent.tail = None
    parent2 = parent.getparent()
    i = parent.index(x)
    j = parent2.index(parent)
    parent2[j+1:j+1] = parent[i:]

def unnest_p(p):
    i = 0
    while i < len(p):
        x = p[i]
        if x.tag in layout_tags: _unnest(x)
        i += 1

def unnest_layout(layout):
    if len(layout): _unnest(layout[0])
    text = layout.text
    if text and not text.isspace():
        layout.text = None
        layout.tail = text

def unnest_content(content):
    i = 0
    while i < len(content):
        x = content[i]
        tag = x.tag
        if tag == 'p': unnest_p(x)
        elif tag == 'column': unnest_element(x)
        elif tag in layout_tags: _unnest(x)
        i += 1

def unnest_element(element):
    i = 0
    while i < len(element):
        x = element[i]
        tag = x.tag
        if tag == 'p': unnest_p(x)
        elif tag in layout_tags: _unnest(x)
        i += 1

# xslt_filename = os.path.join(os.path.dirname(__file__), 'transform.xslt')
# xslt = etree.XSLT(etree.parse(xslt_filename))

def move_content(target, source_list):
    for source in source_list:
        text = source.text
        if text and not text.isspace():
            if len(target):
                last = target[-1]
                last.tail = (last.tail or '') + text
            else: target.text = (target.text or '') + text
        target.extend(source[:])

def make_column(grid, source, first=False):
    column = SubElement(grid, 'div')
    if first: column.set('class', 'yui-u first')
    else: column.set('class', 'yui-u')
    move_content(column, [ source ])

yui_patterns = {
    '2/3-1/3' : 'yui-gc',
    '1/3-2/3' : 'yui-gd',
    '3/4-1/4' : 'yui-ge',
    '1/4-3/4' : 'yui-gf'
    }

def transform(html):
    head = html.find('head')
    body = html.find('body')
    css_links = [ link for link in head.findall('link')
                       if link.get('rel') == 'stylesheet'
                       and link.get('type') == 'text/css' ]
    styles = head.findall('style')
    layout = body.find('layout')
    layout_width = layout is not None and layout.get('width') or None
    header_list = body.findall('header')
    footer_list = body.findall('footer')
    sidebar_list = body.findall('sidebar')
    sidebar_left = True
    sidebar_first = False
    sidebar_width = None
    for sidebar in reversed(sidebar_list):
        if sidebar.get('left') is not None: sidebar_left = True
        elif sidebar.get('right') is not None: sidebar_left = False
        else:
            align = sidebar.get('align')
            if align == 'left': sidebar_left = True
            elif align == 'right': sidebar_right = True
        sidebar_width = sidebar.get('width', sidebar_width)
        sidebar_first = sidebar_first or sidebar.get('first') is not None
    content_list = body.findall('content')
    has_layout = (header_list or footer_list or sidebar_list or content_list
                  or layout is not None)
    width = 0
    if not css_links and not styles:
        pony_static_dir = '/pony/static'
        css1 = pony_static_dir + '/yui/reset-fonts-grids/reset-fonts-grids.css'
        css2 = pony_static_dir + '/css/pony-default.css'
        for css in (css1, css2):
          SubElement(head, 'link', rel='stylesheet', type='text/css', href=css)
        if layout_width is not None \
          and layout_width not in ('750', '800x600', '950', '1024x768'):
            try: width = float(layout_width)
            except ValueError: pass
            else:
                if width:
                    em_width = width / 13
                    em_width_ie = em_width * 0.9759
                    SubElement(head, 'style').text = (
                        '#doc-custom { margin:auto; text-align: left; '
                        'width: %.4fem; *width: %.4fem; '
                        'min_width: %spx; }' % (em_width, em_width_ie, width))
    if has_layout:
        doc = html.makeelement('div')
        if layout_width in ('750', '800x600'): doc.set('id', 'doc')
        elif layout_width in ('950', '1024x768'): doc.set('id', 'doc2')
        elif not width: doc.set('id', 'doc3')
        else: doc.set('id', 'doc-custom')
        if not sidebar_list: doc.set('class', 'yui-t7')
        elif sidebar_left:
            if sidebar_width == '160': doc.set('class', 'yui-t1')
            elif sidebar_width == '300': doc.set('class', 'yui-t3')
            else: doc.set('class', 'yui-t2')
        else:
            if sidebar_width == '240': doc.set('class', 'yui-t5')
            elif sidebar_width == '300': doc.set('class', 'yui-t6')
            else: doc.set('class', 'yui-t4')
        hd = SubElement(doc, 'div', id='hd')
        hd.set('class', 'pony-header')
        move_content(hd, header_list)
        bd = SubElement(doc, 'div', id='bd')
        ft = SubElement(doc, 'div', id='ft')
        ft.set('class', 'pony-footer')
        move_content(ft, footer_list)
        if sidebar_first:
            sb = SubElement(bd, 'div')
            main = SubElement(bd, 'div', id='yui-main')
        else:
            main = SubElement(bd, 'div', id='yui-main')
            sb = SubElement(bd, 'div')
        sb.set('class', 'yui-b pony-sidebar')
        move_content(sb, sidebar_list)
        main2 = SubElement(main, 'div')
        main2.set('class', 'yui-b pony-content')
        for content in content_list:
            pattern = content.get('pattern')
            column_list = content.findall('column')
            col_count = len(column_list)
            if col_count == 1:
                move_content(main2, column_list)
                continue
            grid = SubElement(main2, 'div')
            if col_count == 2:
                grid.set('class', yui_patterns.get(pattern, 'yui-g'))
                make_column(grid, column_list[0], True)
                make_column(grid, column_list[1])
            elif col_count == 3:
                if pattern == '1/4-1/4-1/2':
                    grid.set('class', 'yui-g')
                    grid2 = SubElement(grid, 'div')
                    grid2.set('class', 'yui-g first')
                    make_column(grid2, column_list[0], True)
                    make_column(grid2, column_list[1])
                    make_column(grid, column_list[2])
                elif pattern == '1/2-1/4-1/4':
                    grid.set('class', 'yui-g')
                    make_column(grid, column_list[0], True)
                    grid2 = SubElement(grid, 'div')
                    grid2.set('class', 'yui-g')
                    make_column(grid2, column_list[1], True)
                    make_column(grid2, column_list[2])
                else:
                    grid.set('class', 'yui-gb')
                    make_column(grid, column_list[0], True)
                    make_column(grid, column_list[1])
                    make_column(grid, column_list[2])
            elif col_count == 4:
                grid2a = SubElement(grid, 'div')
                grid2a.set('class', 'yui-g first')
                make_column(grid2a, column_list[0], True)
                make_column(grid2a, column_list[1])
                grid2b = SubElement(grid, 'div')
                grid2b.set('class', 'yui-g')
                make_column(grid2b, column_list[2], True)
                make_column(grid2b, column_list[3])
            else:
                p = SubElement(grid, 'p')
                p.text = 'Wrong column count: %d' % col_count
        body[:] = [ doc ]
        body.text = None
    elif not css_links and not styles:
        doc = html.makeelement('div', id='doc3')
        doc.set('class', 'yui-t7')
        bd = SubElement(doc, 'div', id='bd')
        main = SubElement(bd, 'div', id='yui-main')
        content = SubElement(main, 'div')
        content.set('class', 'yui-b pony-content')
        move_content(content, [ body ])
        body[:] = [ doc ]
        body.text = None
    # else: pass
    return html

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
