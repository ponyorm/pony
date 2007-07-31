from lxml.etree import SubElement
from pony.layout import move_content

def make_column(grid, source, first=False):
    column = SubElement(grid, 'div')
    if first: column.set('class', 'yui-u pony-column first')
    else: column.set('class', 'yui-u pony-column')
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
        for css in [ '/yui/reset-fonts-grids/reset-fonts-grids.css',
                     '/css/layouts/yui.css',
                     '/css/default.css' ]:
          SubElement(head, 'link', rel='stylesheet', type='text/css',
                     href= pony_static_dir + css)
        if layout_width is not None \
        and layout_width not in ('750', '950'):
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
        if layout_width == '750': doc.set('id', 'doc')
        elif layout_width == '950': doc.set('id', 'doc2')
        elif not width: doc.set('id', 'doc3')
        else: doc.set('id', 'doc-custom')
        if not sidebar_list:
            doc.set('class', 'yui-t7')
            body.set('class', 'fluid-content')
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
        if sidebar_first: sb = SubElement(bd, 'div')
        main = SubElement(bd, 'div', id='yui-main')
        if sidebar_list and not sidebar_first: sb = SubElement(bd, 'div')
        if sidebar_list:
            sb.set('class', 'yui-b pony-sidebar '
                            + (sidebar_left and 'left' or 'right'))
            move_content(sb, sidebar_list)
        main2 = SubElement(main, 'div')
        if sidebar_list:
            if sidebar_left: main2.set('class', 'yui-b pony-content right')
            else: main2.set('class', 'yui-b pony-content left')
        else: main2.set('class', 'yui-b pony-content')

        for row in row_list:
            pattern = row.get('pattern')
            column_list = row.findall('column')
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
        ft = SubElement(doc, 'div', id='ft')
        ft.set('class', 'pony-footer')
        move_content(ft, footer_list)
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
