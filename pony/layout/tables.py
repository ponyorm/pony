from lxml.etree import SubElement
from pony.layout import move_content

column_width_map = {
    '1/2' : '50%',
    '1/3' : '33%', '2/3' : '66%',
    '1/4' : '25%', '3/4' : '75%',
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
    if layout_width == '800x600': layout_width = '750'
    elif layout_width == '1024x768': layout_width = '950'
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
    row_list = body.findall('row')
    has_layout = (header_list or footer_list or sidebar_list or row_list
                  or layout is not None)
    width = 0
    if not css_links and not styles:
        pony_static_dir = '/pony/static'
        for css in  [ '/yui/reset/reset-min.css',
                      '/yui/fonts/fonts-min.css',
                      '/css/layouts/tables.css',
                      '/css/default.css' ]:
          SubElement(head, 'link', rel='stylesheet', type='text/css',
                     href=pony_static_dir + css)
    if has_layout:
        table = html.makeelement('table', id='doc',
                                 align='center', width='100%')
        if layout_width: table.set('width', layout_width)
        else: body.set('class', 'fluid-content')
        tr = SubElement(table, 'tr', id='hd')
        td = SubElement(tr, 'td', colspan='2')
        td.set('class', 'pony-header')
        move_content(td, header_list)
        tr = SubElement(table, 'tr', id='bd', height='100%')
        if sidebar_list:
            if sidebar_left:
                sb = SubElement(tr, 'td', valign='top')
                sb.set('class', 'pony-sidebar left')
                main = SubElement(tr, 'td', valign='top')
                main.set('class', 'pony-content right')
            else:
                main = SubElement(tr, 'td', valign='top')
                main.set('class', 'pony-content left')
                sb = SubElement(tr, 'td', valign='top')
                sb.set('class', 'pony-sidebar right')
            sb.set('width', sidebar_width or '180')
            move_content(sb, sidebar_list)
        else:
            main = SubElement(tr, 'td', width='100%', valign='top')
            main.set('class', 'pony-content')
        if not row_list or len(row_list) == 1 and len(row_list[0]) == 1:
            move_content(main, row_list)
        else:
            for row in row_list:
                grid = SubElement(main, 'table', width='100%')
                tr = SubElement(grid, 'tr')
                for i, column in enumerate(row):
                    td = SubElement(tr, 'td', valign='top')
                    td.set('class', 'pony-column' + (not i and ' first' or ''))
                    column_width = column.get('width')
                    if column_width is not None:
                        td_width = column_width_map.get(column_width)
                        td.set('width', td_width or column_width)
                    move_content(td, [ column ])
        tr = SubElement(table, 'tr', id='ft')
        td = SubElement(tr, 'td', colspan='2')
        td.set('class', 'pony-footer')
        move_content(td, footer_list)
        body[:] = [ table ]
        body.text = None
    # else: pass
    return html
