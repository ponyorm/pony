from cStringIO import StringIO
from binascii import unhexlify

try: import Image, ImageDraw, ImageColor
except ImportError: PIL = False
else: PIL = True

from pony.web import http
from pony.utils import cached

def _decode_colour(colour):
    try: colour = ImageColor.colormap[colour][1:]
    except KeyError: pass
    size = len(colour)
    if size in (3, 4): colour = ''.join(char+char for char in colour)
    elif size not in (6, 8): raise ValueError
    try: return tuple(map(ord, unhexlify(colour)))
    except: raise ValueError

def _decode_png_colours(colour1, colour2):
    if colour1 is not None:
        colour1 = _decode_colour(colour1)
        if colour2 is not None: colour2 = _decode_colour(colour2)
        elif len(colour1) == 3: colour1, colour2 = colour1 + (255,), colour1 + (0,)
        elif colour1[-1] == 255: colour2 = colour1[:-1] + (0,)
        elif colour1[-1] == 0: colour2 = colour1[:-1] + (255,)
        else: raise ValueError
    else:
        colour1 = (255, 255, 255, 255)
        colour2 = (255, 255, 255, 0)
    return colour1, colour2

def _circle_image(radius, colour, bgcolour):
    if not PIL: raise ValueError
    try: radius = int(radius)
    except: raise ValueError
    if not 2 <= radius <= 100: raise ValueError
    if len(colour) != len(bgcolour): raise ValueError
    mode = len(colour)==3 and 'RGB' or 'RGBA'

    quarter = Image.new(mode, (radius*4, radius*4), bgcolour)
    draw = ImageDraw.Draw(quarter)
    draw.pieslice((0, 0, radius*8, radius*8), 180, 270, fill=colour)
    quarter = quarter.resize((radius, radius), Image.ANTIALIAS)

    circle = Image.new(mode, (radius*2, radius*2), 0)
    circle.paste(quarter, (0, 0, radius, radius))
    circle.paste(quarter.rotate(90), (0, radius, radius, radius*2))
    circle.paste(quarter.rotate(180), (radius, radius, radius*2, radius*2))
    circle.paste(quarter.rotate(270), (radius, 0, radius*2, radius))
    return circle

@http('/pony/images/circle$radius.png',           type='image/png')
@http('/pony/images/circle$radius-$colour.png',           type='image/png')
@http('/pony/images/circle$radius-$colour-$bgcolour.png', type='image/png')
@cached
def png_circle(radius, colour='000000', bgcolour=None):
    try:
        colour, bgcolour = _decode_png_colours(colour, bgcolour)                
        img = _circle_image(radius, colour, bgcolour)
        io = StringIO()
        img.save(io, 'PNG')
        return io.getvalue()
    except ValueError: raise http.NotFound

@http('/pony/images/hole$radius.png',           type='image/png')
@http('/pony/images/hole$radius-$bgcolour.png', type='image/png')
@cached
def png_hole(radius, bgcolour='ffffffff'):
    try:
        bgcolour, colour = _decode_png_colours(bgcolour, None)
        img = _circle_image(radius, colour, bgcolour)
        io = StringIO()
        img.save(io, 'PNG')
        return io.getvalue()
    except ValueError: raise http.NotFound

@http('/pony/images/circle$radius.gif',           type='image/gif')
@http('/pony/images/circle$radius-$colour.gif',           type='image/gif')
@http('/pony/images/circle$radius-$colour-$bgcolour.gif', type='image/gif')
@cached
def gif_circle(radius, colour='000000', bgcolour='ffffff'):
    try:
        colour = _decode_colour(colour)
        if len(colour) != 3: raise ValueError
        bgcolour=_decode_colour(bgcolour)
        img = _circle_image(radius, colour, bgcolour)
        img = img.convert("P", dither=Image.NONE, palette=Image.ADAPTIVE)
        io = StringIO()
        img.save(io, 'GIF')
        return io.getvalue()
    except ValueError: raise http.NotFound

@http('/pony/images/hole$radius.gif',           type='image/gif')
@http('/pony/images/hole$radius-$bgcolour.gif', type='image/gif')
@cached
def gif_hole(radius, bgcolour='ffffff'):
    if not PIL: raise http.NotFound
    try: radius = int(radius)
    except: raise http.NotFound
    if not 2 <= radius <= 100: raise http.NotFound
    bgcolour = _decode_colour(bgcolour)
    if len(bgcolour) != 3: raise ValueError

    quarter = Image.new("P", (radius, radius), 0)
    draw = ImageDraw.Draw(quarter)
    draw.pieslice((0, 0, radius*2, radius*2), 180, 270, fill=1)

    circle = Image.new("P", (radius*2, radius*2), 0)
    circle.paste(quarter, (0, 0, radius, radius))
    circle.paste(quarter.rotate(90), (0, radius, radius, radius*2))
    circle.paste(quarter.rotate(180), (radius, radius, radius*2, radius*2))
    circle.paste(quarter.rotate(270), (radius, 0, radius*2, radius))

    if bgcolour == (0, 0, 0): palette = (255, 255, 255)
    else: palette = bgcolour + (0, 0, 0)
    circle.putpalette(palette)
    io = StringIO()
    circle.save(io, 'GIF', transparency=1)
    return io.getvalue()

def _calc_colours(count, start_colour, end_colour):
    assert len(start_colour) == len(end_colour)
    last = count - 1
    if len(start_colour) == 3:
        r1, g1, b1 = start_colour
        r2, g2, b2 = end_colour
        r, g, b = r2-r1, g2-g1, b2-b1
        for i in range(count): yield i, (r1+r*i/last, g1+g*i/last, b1+b*i/last)
    elif len(start_colour) == 4:
        r1, g1, b1, t1 = start_colour
        r2, g2, b2, t2 = end_colour
        r, g, b, t = r2-r1, g2-g1, b2-b1, t2-t1
        for i in range(count): yield i, (r1+r*i/last, g1+g*i/last, b1+b*i/last, t1+t*i/last)
    else: assert False

if Image.VERSION >= '1.1.6':

    def _draw_gradient(img, start, stop, start_colour, end_colour):
        pixels = img.load()
        for i, colour in _calc_colours(stop-start, start_colour, end_colour):
            pixels[start + i, 0] = colour
else:

    def _draw_gradient(img, start, stop, start_colour, end_colour):
        putpixel = img.img.putpixel
        for i, colour in _calc_colours(stop-start, start_colour, end_colour):
            putpixel((start + i, 0), colour)

def _line(format, horiz, data):
    segments = []
    mode = None
    total_length = 0
    for item in data.split('+'):
        item = item.split('-')
        if len(item) == 2: item.append(None)
        elif len(item) != 3: raise ValueError #http.NotFound
        length, colour, colour2 = item
        try: length  = int(length)
        except: raise ValueError #http.NotFound
        else:
            if length <= 0: raise ValueError #http.NotFound
            total_length += length
        colour = _decode_colour(colour)
        if colour2 is not None:
            colour2 = _decode_colour(colour2)
            if len(colour) != len(colour2): raise ValueError #http.NotFound
        if mode is None: mode = len(colour)==3 and 'RGB' or 'RGBA'
        elif mode == 'RGB' and len(colour) != 3: raise ValueError #http.NotFound
        elif mode == 'RGBA' and len(colour) != 4: raise ValueError #http.NotFound
        segments.append((length, colour, colour2))
    if not 0 < total_length <= 10000: raise ValueError #http.NotFound
    if format == 'GIF' and mode == 'RGBA': raise ValueError #http.NotFound
    img = Image.new(mode, (total_length, 1), mode=='RGB' and (0, 0, 0) or (0, 0, 0, 255))
    draw = ImageDraw.Draw(img)
    start = 0
    for length, colour, colour2 in segments:
        if colour2 is None: _draw_gradient(img, start, start+length, colour, colour)
        else: _draw_gradient(img, start, start+length, colour, colour2)
        start += length
    if not horiz: img = img.rotate(270)
    # if format == 'GIF': img  = img.convert("P", dither=Image.NONE, palette=Image.ADAPTIVE)
    io = StringIO()
    img.save(io, format)
    return io.getvalue()

@http('/pony/images/hline$data.png', type='image/png')
@cached
def hline_png(data):
    return _line('PNG', True, data)

@http('/pony/images/hline$data.gif', type='image/gif')
@cached
def hline_gif(data):
    return _line('GIF', True, data)

@http('/pony/images/vline$data.png', type='image/png')
@cached
def vline_png(data):
    return _line('PNG', False, data)

@http('/pony/images/vline$data.gif', type='image/gif')
@cached
def vline_gif(data):
    return _line('GIF', False, data)

@http('/pony/images/pixel.png',         type='image/png')
@http('/pony/images/pixel-$colour.png', type='image/png')
@cached
def pixel_png(colour='00000000'):
    try:
        colour = _decode_colour(colour)
        mode = len(colour)==6 and 'RGB' or 'RGBA'
        img = Image.new(mode, (1, 1), colour)
        io = StringIO()
        img.save(io, 'PNG')
        return io.getvalue()
    except ValueError: raise http.NotFound

@http('/pony/images/pixel.gif',         type='image/gif')
@http('/pony/images/pixel-$colour.gif', type='image/gif')
@cached
def pixel_gif(colour=None):
    try:
        if colour is not None: colour = _decode_colour(colour)
        img = Image.new("P", (1, 1))
        img.putpalette(colour or (255, 255, 255))
        img.putpixel((0, 0), 0)
        io = StringIO()
        if colour is None: img.save(io, 'GIF', transparency=0)
        else: img.save(io, 'GIF')
        return io.getvalue()
    except ValueError: raise http.NotFound
