from lxml.etree import SubElement, Comment
from pony.layout import move_content

pony_static_dir = '/pony/static'

def transform(html):
    head = html.find('head')
    body = html.find('body')
    links = head.findall('link')
    css_links = [ link for link in links
                       if link.get('rel') == 'stylesheet'
                       and link.get('type') == 'text/css' ]
    favicon_links = [ link for link in links if 'icon' in link.get('rel', '').split() ]
    if not favicon_links:
        for rel in ('shortcut icon', 'icon'):
            SubElement(head, 'link', rel=rel, type='image/vnd.microsoft.icon',
                       href=pony_static_dir + '/favicon.ico')
    styles = head.findall('style')
    if not css_links and not styles:
          SubElement(head, 'link', rel='stylesheet', type='text/css', media="screen, projection",
                     href= pony_static_dir + '/blueprint/screen.css')
          SubElement(head, 'link', rel='stylesheet', type='text/css', media="print",
                     href= pony_static_dir + '/blueprint/print.css')
          head.append(Comment('start of IE hack'))
          SubElement(head, 'link', rel='stylesheet', type='text/css', media="screen, projection",
                     href= pony_static_dir + '/blueprint/ie.css')
          head.append(Comment('end of IE hack'))
          content = html.makeelement('div')
          content.set('class', 'container')
          move_content(content, [ body ])
          body[:] = [ content ]
          body.text = None
    return html
