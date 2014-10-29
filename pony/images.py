from __future__ import absolute_import, print_function
from pony.py23compat import imap

from cStringIO import StringIO
from binascii import unhexlify

try: import Image, ImageDraw, ImageColor
except ImportError: PIL = False
else: PIL = True

from pony.web import http, no_cookies
from pony.utils import cached

def _decode_color(color):
    if not PIL: raise ValueError
    try: color = ImageColor.colormap[color][1:]
    except KeyError: pass
    size = len(color)
    if size in (3, 4): color = ''.join(char+char for char in color)
    elif size not in (6, 8): raise ValueError
    try: return tuple(imap(ord, unhexlify(color)))
    except: raise ValueError

def _decode_png_colors(color1, color2):
    if not PIL: raise ValueError
    if color1 is not None:
        color1 = _decode_color(color1)
        if color2 is not None: color2 = _decode_color(color2)
        elif len(color1) == 3: color1, color2 = color1 + (255,), color1 + (0,)
        elif color1[-1] == 255: color2 = color1[:-1] + (0,)
        elif color1[-1] == 0: color2 = color1[:-1] + (255,)
        else: raise ValueError
    else:
        color1 = (255, 255, 255, 255)
        color2 = (255, 255, 255, 0)
    return color1, color2

def _circle_image(radius, color, bgcolor):
    if not PIL: raise ValueError
    try: radius = int(radius)
    except: raise ValueError
    if not 2 <= radius <= 100: raise ValueError
    if len(color) != len(bgcolor): raise ValueError
    mode = 'RGB' if len(color) == 3 else 'RGBA'

    quarter = Image.new(mode, (radius*4, radius*4), bgcolor)
    draw = ImageDraw.Draw(quarter)
    draw.pieslice((0, 0, radius*8, radius*8), 180, 270, fill=color)
    quarter = quarter.resize((radius, radius), Image.ANTIALIAS)

    circle = Image.new(mode, (radius*2, radius*2), 0)
    circle.paste(quarter, (0, 0, radius, radius))
    circle.paste(quarter.rotate(90), (0, radius, radius, radius*2))
    circle.paste(quarter.rotate(180), (radius, radius, radius*2, radius*2))
    circle.paste(quarter.rotate(270), (radius, 0, radius*2, radius))
    return circle

@http('/pony/images/circle$radius.png',                 type='image/png')
@http('/pony/images/circle$radius-$color.png',          type='image/png')
@http('/pony/images/circle$radius-$color-$bgcolor.png', type='image/png')
@no_cookies
@cached
def png_circle(radius, color='000000', bgcolor=None):
    try:
        color, bgcolor = _decode_png_colors(color, bgcolor)
        img = _circle_image(radius, color, bgcolor)
        io = StringIO()
        img.save(io, 'PNG')
        return io.getvalue()
    except ValueError: raise http.NotFound

@http('/pony/images/hole$radius.png',           type='image/png')
@http('/pony/images/hole$radius-$bgcolor.png', type='image/png')
@no_cookies
@cached
def png_hole(radius, bgcolor='ffffffff'):
    try:
        bgcolor, color = _decode_png_colors(bgcolor, None)
        img = _circle_image(radius, color, bgcolor)
        io = StringIO()
        img.save(io, 'PNG')
        return io.getvalue()
    except ValueError: raise http.NotFound

@http('/pony/images/circle$radius.gif',                 type='image/gif')
@http('/pony/images/circle$radius-$color.gif',          type='image/gif')
@http('/pony/images/circle$radius-$color-$bgcolor.gif', type='image/gif')
@no_cookies
@cached
def gif_circle(radius, color='000000', bgcolor='ffffff'):
    try:
        color = _decode_color(color)
        if len(color) != 3: raise ValueError
        bgcolor=_decode_color(bgcolor)
        img = _circle_image(radius, color, bgcolor)
        img = img.convert("P", dither=Image.NONE, palette=Image.ADAPTIVE)
        io = StringIO()
        img.save(io, 'GIF')
        return io.getvalue()
    except ValueError: raise http.NotFound

@http('/pony/images/hole$radius.gif',           type='image/gif')
@http('/pony/images/hole$radius-$bgcolor.gif', type='image/gif')
@no_cookies
@cached
def gif_hole(radius, bgcolor='ffffff'):
    if not PIL: raise http.NotFound
    try: radius = int(radius)
    except: raise http.NotFound
    if not 2 <= radius <= 100: raise http.NotFound
    bgcolor = _decode_color(bgcolor)
    if len(bgcolor) != 3: raise ValueError

    quarter = Image.new("P", (radius, radius), 0)
    draw = ImageDraw.Draw(quarter)
    draw.pieslice((0, 0, radius*2, radius*2), 180, 270, fill=1)

    circle = Image.new("P", (radius*2, radius*2), 0)
    circle.paste(quarter, (0, 0, radius, radius))
    circle.paste(quarter.rotate(90), (0, radius, radius, radius*2))
    circle.paste(quarter.rotate(180), (radius, radius, radius*2, radius*2))
    circle.paste(quarter.rotate(270), (radius, 0, radius*2, radius))

    if bgcolor == (0, 0, 0): palette = (255, 255, 255)
    else: palette = bgcolor + (0, 0, 0)
    circle.putpalette(palette)
    io = StringIO()
    circle.save(io, 'GIF', transparency=1)
    return io.getvalue()

def _calc_colors(count, start_color, end_color):
    assert len(start_color) == len(end_color)
    last = count - 1
    if len(start_color) == 3:
        r1, g1, b1 = start_color
        r2, g2, b2 = end_color
        r, g, b = r2-r1, g2-g1, b2-b1
        for i in range(count): yield i, (r1+r*i/last, g1+g*i/last, b1+b*i/last)
    elif len(start_color) == 4:
        r1, g1, b1, t1 = start_color
        r2, g2, b2, t2 = end_color
        r, g, b, t = r2-r1, g2-g1, b2-b1, t2-t1
        for i in range(count): yield i, (r1+r*i/last, g1+g*i/last, b1+b*i/last, t1+t*i/last)
    else: assert False  # pragma: no cover

if PIL and Image.VERSION >= '1.1.6':

    def _draw_gradient(img, start, stop, start_color, end_color):
        pixels = img.load()
        for i, color in _calc_colors(stop-start, start_color, end_color):
            pixels[start + i, 0] = color
else:

    def _draw_gradient(img, start, stop, start_color, end_color):
        putpixel = img.img.putpixel
        for i, color in _calc_colors(stop-start, start_color, end_color):
            putpixel((start + i, 0), color)

def _line(format, horiz, data):
    if not PIL: raise http.NotFound
    segments = []
    mode = None
    total_length = 0
    for item in data.split('+'):
        item = item.split('-')
        if len(item) == 2: item.append(None)
        elif len(item) != 3: raise http.NotFound
        length, color, color2 = item
        try: length  = int(length)
        except: raise http.NotFound
        else:
            if length <= 0: raise http.NotFound
            total_length += length
        color = _decode_color(color)
        if color2 is not None:
            color2 = _decode_color(color2)
            if len(color) != len(color2): raise http.NotFound
        if mode is None: mode = 'RGB' if len(color) == 3 else 'RGBA'
        elif mode == 'RGB' and len(color) != 3: raise http.NotFound
        elif mode == 'RGBA' and len(color) != 4: raise http.NotFound
        segments.append((length, color, color2))
    if not 0 < total_length <= 10000: raise http.NotFound
    if format == 'GIF' and mode == 'RGBA': raise http.NotFound
    img = Image.new(mode, (total_length, 1), (0, 0, 0) if mode=='RGB' else (0, 0, 0, 255))
    draw = ImageDraw.Draw(img)
    start = 0
    for length, color, color2 in segments:
        if color2 is None: _draw_gradient(img, start, start+length, color, color)
        else: _draw_gradient(img, start, start+length, color, color2)
        start += length
    if not horiz: img = img.rotate(270)
    # if format == 'GIF': img  = img.convert("P", dither=Image.NONE, palette=Image.ADAPTIVE)
    io = StringIO()
    img.save(io, format)
    return io.getvalue()

@http('/pony/images/hline$data.png', type='image/png')
@no_cookies
@cached
def hline_png(data):
    return _line('PNG', True, data)

@http('/pony/images/hline$data.gif', type='image/gif')
@no_cookies
@cached
def hline_gif(data):
    return _line('GIF', True, data)

@http('/pony/images/vline$data.png', type='image/png')
@no_cookies
@cached
def vline_png(data):
    return _line('PNG', False, data)

@http('/pony/images/vline$data.gif', type='image/gif')
@no_cookies
@cached
def vline_gif(data):
    return _line('GIF', False, data)

@http('/pony/images/pixel.png',        type='image/png')
@http('/pony/images/pixel-$color.png', type='image/png')
@no_cookies
@cached
def pixel_png(color='00000000'):
    if not PIL: raise http.NotFound
    try:
        color = _decode_color(color)
        mode = 'RGB' if len(color) == 6 else 'RGBA'
        img = Image.new(mode, (1, 1), color)
        io = StringIO()
        img.save(io, 'PNG')
        return io.getvalue()
    except ValueError: raise http.NotFound

@http('/pony/images/pixel.gif',        type='image/gif')
@http('/pony/images/pixel-$color.gif', type='image/gif')
@no_cookies
@cached
def pixel_gif(color=None):
    if not PIL: raise http.NotFound
    try:
        if color is not None: color = _decode_color(color)
        img = Image.new("P", (1, 1))
        img.putpalette(color or (255, 255, 255))
        img.putpixel((0, 0), 0)
        io = StringIO()
        if color is None: img.save(io, 'GIF', transparency=0)
        else: img.save(io, 'GIF')
        return io.getvalue()
    except ValueError: raise http.NotFound

@http('/pony/images/tab.png', type='image/png')
@http('/pony/images/tab-$width-$height.png', type='image/png')
@no_cookies
@cached
def tab_png(width=300, height=200, radius=5, topcolor=None, bottomcolor=None, bordercolor=None, backcolor=None):
    if not PIL: raise http.NotFound
    topcolor =  _decode_color(topcolor) if topcolor else (255, 255, 255, 255)
    bottomcolor = _decode_color(bottomcolor) if bottomcolor else (217, 234, 244, 255)
    bordercolor = _decode_color(bordercolor) if bordercolor else (60, 85, 107, 255)
    backcolor = _decode_color(backcolor) if backcolor else (255, 255, 255, 0)
    gradients = []
    for i, color in _calc_colors(height/8, topcolor, bottomcolor): gradients.append(color)

    corner = Image.new('RGBA', (radius*4, radius*4), backcolor)
    pixels = corner.load()
    draw = ImageDraw.Draw(corner)
    draw.pieslice((0, 0, radius*8, radius*8), 180, 270, fill=bordercolor)
    draw.pieslice((4, 4, radius*8-4, radius*8-4), 180, 270, fill=topcolor)
    selected_corner = corner.copy().resize((radius, radius), Image.ANTIALIAS)
    for x in range(0, radius*4):
        for y in range(0, radius*4):
            color = pixels[x, y]
            if color == topcolor: pixels[x, y] = gradients[y/4]
    corner = corner.resize((radius, radius), Image.ANTIALIAS)

    img = Image.new('RGBA', (width, height), topcolor)
    draw = ImageDraw.Draw(img)
    for i, color in enumerate(gradients):
        draw.line((0, i, width, i), fill=color)
    draw.rectangle((0, height/8, width, height/4), fill=bottomcolor)
    draw.rectangle((0, height/2, width, height), fill=backcolor)
    draw.line((0, 0, width, 0), fill=bordercolor)
    draw.line((0, 0, 0, height/2), fill=bordercolor)
    draw.line((0, height/4-1, width, height/4-1), fill=backcolor)
    draw.line((0, height/4, width, height/4), fill=bordercolor)
    img.paste(corner, (0, 0))
    img.paste(selected_corner, (0, height/4))
    right = img.crop((0, 0, 40, height/2))
    right = right.transpose(Image.FLIP_LEFT_RIGHT)
    img.paste(right, (width-40, height/2))
    io = StringIO()
    img.save(io, 'PNG')
    return io.getvalue()
