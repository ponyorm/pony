import re

from cStringIO import StringIO

try: import Image, ImageDraw
except ImportError: PIL = False
else: PIL = True

from pony.templating import template, cycle
if __name__ == '__main__':
    def cached(func): return func
    def http(*args, **kwargs): return lambda x: x
else:
    from pony.utils import cached
    from pony.web import http

comment_re = re.compile(r'/[*](?:[^*]|[*][^/])*[*]/')

def compress(s):
    s = comment_re.sub(' ', s)
    s = ' '.join(s.split())
    s = s.replace(': ', ':').replace(', ', ',').replace('; ', ';').replace('{ ', '{').replace('} ', '}')
    return s.replace('{', '\n{').replace('}', '}\n')
if __name__ == '__main__':
    def compress(s): return s

@http('/pony/blueprint/grid.png', type='image/png')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/grid.png', type='image/png')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/$ns/grid.png', type='image/png')
@cached
def grid_background(column_count=24, column_width=30, gutter_width=10, ns=''):
    if not PIL: raise http.NotFound
    im = Image.new('RGB', (column_width+gutter_width, 18), (255, 255, 255))
    draw = ImageDraw.Draw(im)
    draw.rectangle((0, 0, column_width-1, 17), fill=(232, 239, 251))
    draw.line((0, 17, column_width+gutter_width, 17), fill=(233, 233, 233))
    io = StringIO()
    im.save(io, 'PNG')
    return io.getvalue()

@http('/pony/blueprint/ie.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/ie.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/$ns/ie.css', type='text/css')
@cached
def ie(column_count=24, column_width=30, gutter_width=10, ns=''):
    return compress(template())

@http('/pony/blueprint/print.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/print.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/$ns/print.css', type='text/css')
@cached
def print_(column_count=24, column_width=30, gutter_width=10, ns=''):
    return compress(template())

@http('/pony/blueprint/screen.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/screen.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/$ns/screen.css', type='text/css')
@cached
def screen(column_count=24, column_width=30, gutter_width=10, ns=''):
    reset_str = compress(reset(column_count, column_width, gutter_width, ns))
    typography_str = compress(typography(column_count, column_width, gutter_width, ns))
    grid_str = compress(grid(column_count, column_width, gutter_width, ns))
    forms_str = compress(forms(column_count, column_width, gutter_width, ns))
    return '\n'.join(('/* screen.css */\n\n/* reset.css */', reset_str,
                      '/* typography.css */', typography_str,
                      '/* grid.css */', grid_str,
                      '/* forms.css */', forms_str))

@http('/pony/blueprint/reset.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/reset.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/$ns/reset.css', type='text/css')
@cached
def reset(column_count=24, column_width=30, gutter_width=10, ns=''):
    return template()

@http('/pony/blueprint/typography.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/typography.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/$ns/typography.css', type='text/css')
@cached
def typography(column_count=24, column_width=30, gutter_width=10, ns=''):
    return template()

@http('/pony/blueprint/grid.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/grid.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/$ns/grid.css', type='text/css')
@cached
def grid(column_count=24, column_width=30, gutter_width=10, ns=''):
    page_width = column_count*(column_width+gutter_width) - gutter_width
    return template()

@http('/pony/blueprint/forms.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/forms.css', type='text/css')
@http('/pony/blueprint/$column_count/$column_width/$gutter_width/$ns/forms.css', type='text/css')
@cached
def forms(column_count=24, column_width=30, gutter_width=10, ns=''):
    return template()


if __name__ == '__main__':
    file('reset_test.css', 'w').write(reset())
    file('typography_test.css', 'w').write(typography())
    file('forms_test.css', 'w').write(forms())
    file('grid_test.css', 'w').write(grid())
    file('ie_test.css', 'w').write(ie())
    file('print_test.css', 'w').write(print_())
