from __future__ import absolute_import, print_function

import re

from pony import options
from pony.templating import Html, StrHtml

element_re = re.compile(r'\s*(?:<!--.*?--\s*>\s*)*(</?\s*([!A-Za-z-]\w*)\b[^>]*>)', re.DOTALL)

header_tags = set("!doctype html head title base script style meta link object".split())

css_re = re.compile('<link\b[^>]\btype\s*=\s*([\'"])text/css\1')

class _UsePlaceholders(Exception): pass

def css_link(link):
    if isinstance(link, basestring): link = (link,)
    elif len(link) > 3: raise TypeError('too many parameters for CSS reference')
    href, media, cond = (link + (None, None))[:3]
    result = '<link rel="stylesheet" href="%s" type="text/css"%s>' \
             % (href, ' media="%s"' % media if media else '')
    if cond: result = '<!--[%s]>%s<![endif]-->' % (cond, result)
    return StrHtml(result)

def css_links(links):
    return StrHtml('\n').join(css_link(link) for link in links)

def script_link(link):
    return StrHtml('<script type="text/javascript" src="%s"></script>') % link

def script_links(links):
    return StrHtml('\n').join(script_link(link) for link in links)

favicon_links = Html('''
<link rel="icon" type="image/x-icon" href="/favicon.ico">
<link rel="shortcut icon" type="image/x-icon" href="/favicon.ico">
''')

def postprocess(content, stylesheets, component_stylesheets, scripts):
    assert isinstance(content, basestring)
    if isinstance(content, (Html, StrHtml)): pass
    elif isinstance(content, str): content = StrHtml(content)
    elif isinstance(content, unicode): content = Html(content)

    if not stylesheets: stylesheets = options.STD_STYLESHEETS
    base_css = css_links(stylesheets)
    if base_css: base_css += StrHtml('\n')
    component_css = css_links(component_stylesheets)
    if component_css: component_css += StrHtml('\n')
    scripts = script_links(scripts)
    if scripts: scripts += StrHtml('\n')

    doctype = ''
    try:
        match = element_re.search(content)
        if match is None or match.group(2).lower() not in header_tags:
            doctype = StrHtml(options.STD_DOCTYPE)
            head = ''
            body = content
        else:
            first_element = match.group(2).lower()

            for match in element_re.finditer(content):
                element = match.group(2).lower()
                if element not in header_tags: break
                last_match = match
            bound = last_match.end(1)
            head = content.__class__(content[:bound])
            body = content.__class__(content[bound:])

            if first_element in ('!doctype', 'html'): raise _UsePlaceholders
            doctype = StrHtml(options.STD_DOCTYPE)

        match = element_re.search(body)
        if match is None or match.group(2).lower() != 'body':
            if 'blueprint' in base_css: body = StrHtml('<div class="container">\n%s\n</div>\n') % body
            body = StrHtml('<body>\n%s</body>') % body

        match = element_re.search(head)
        if match is not None and match.group(2).lower() == 'head': raise _UsePlaceholders
        if css_re.search(head) is not None: base_css = ''
        head = StrHtml('<head>') + favicon_links + base_css + head + component_css + scripts + StrHtml('</head>')

    except _UsePlaceholders:
        head = head.replace(options.BASE_STYLESHEETS_PLACEHOLDER, base_css, 1)
        head = head.replace(options.COMPONENT_STYLESHEETS_PLACEHOLDER, component_css, 1)
        head = head.replace(options.SCRIPTS_PLACEHOLDER, scripts, 1)
        head = content.__class__(head)

    if doctype: return StrHtml('\n').join([doctype, head, body])
    else: return StrHtml('\n').join([head, body])
