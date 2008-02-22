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

def _line_image(format, horiz, length, colour, colour2=None):
    try: length = int(length)
    except: raise ValueError
    if not 0 <= length <= 10000: raise ValueError
    colour = _decode_colour(colour)
    if colour2 is not None: colour2 = _decode_colour(colour2)
    mode = len(colour)==6 and 'RGB' or 'RGBA'
    img = Image.new(mode, (length, 1), colour)
    if colour2 is None: pass
    elif len(colour2) != len(colour): raise ValueError
    else:
        r1, g1, b1 = colour
        r2, g2, b2 = colour2
        if Image.VERSION >= '1.1.6':
            pixels = img.load()
            for i in range(length):
                pixels[i, 0] = ((r1 + r2*i/length), (g1 + g2*i/length), (b1 + b2*i/length))
        else:
            putpixel = im.im.putpixel
            for i in range(length):
                putpixel((i, 0), ((r1 + r2*i/length), (g1 + g2*i/length), (b1 + b2*i/length)))

    if not horiz: img = img.rotate(270)
    # if format == 'GIF': img  = img.convert("P", dither=Image.NONE, palette=Image.ADAPTIVE)
    io = StringIO()
    img.save(io, format)
    return io.getvalue()

@http('/pony/images/hline$length-$colour.png',          type='image/png')
@http('/pony/images/hline$length-$colour-$colour2.png', type='image/png')
@cached
def hline_png(length, colour, colour2=None):
    try: return _line_image('PNG', True, length, colour, colour2)
    except ValueError: raise http.NotFound

@http('/pony/images/hline$length-$colour.gif',          type='image/gif')
@http('/pony/images/hline$length-$colour-$colour2.gif', type='image/gif')
@cached
def hline_gif(length, colour, colour2=None):
    try: return _line_image('GIF', True, length, colour, colour2)
    except ValueError: raise http.NotFound

@http('/pony/images/vline$length-$colour.png',          type='image/png')
@http('/pony/images/vline$length-$colour-$colour2.png', type='image/png')
@cached
def vline_png(length, colour, colour2=None):
    try: return _line_image('PNG', False, length, colour, colour2)
    except ValueError: raise http.NotFound

@http('/pony/images/vline$length-$colour.gif',          type='image/gif')
@http('/pony/images/vline$length-$colour-$colour2.gif', type='image/gif')
@cached
def vline_gif(length, colour, colour2=None):
    try: return _line_image('GIF', False, length, colour, colour2)
    except ValueError: raise http.NotFound

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
