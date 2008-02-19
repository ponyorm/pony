from cStringIO import StringIO
from binascii import unhexlify

try: import Image, ImageDraw, ImageColor
except ImportError: PIL = False
else: PIL = True

from pony.web import http
from pony.utils import simple_decorator

_cache = {}
MAX_CACHE_SIZE = 1000

@simple_decorator
def cached(f, *args, **keyargs):
    key = (f, args, tuple(sorted(keyargs.items())))
    value = _cache.get(key)
    if value is not None: return value
    if len(_cache) == MAX_CACHE_SIZE: _cache.clear()
    return _cache.setdefault(key, f(*args, **keyargs))

def _decode_colour(colour):
    try: colour = ImageColor.colormap[colour][1:]
    except KeyError: pass
    size = len(colour)
    if size in (3, 4): colour = ''.join(char+char for char in colour)
    elif size not in (6, 8): raise ValueError
    try: return tuple(map(ord, unhexlify(colour)))
    except: raise ValueError

def _circle_image(radius, colour, bgcolour):
    if not PIL: raise ValueError
    try: radius = int(radius)
    except: raise ValueError
    if not 0 <= radius <= 100: raise ValueError
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

@http('/pony/images/circle$radius.png',                   type='image/png')
@http('/pony/images/circle$radius-$colour.png',           type='image/png')
@http('/pony/images/circle$radius-$colour-$bgcolour.png', type='image/png')
@cached
def circle_png(radius, colour=None, bgcolour=None):
    if colour is not None:
        colour = _decode_colour(colour)
        if bgcolour is not None: bgcolour = _decode_colour(bgcolour)
        elif len(colour) == 3: colour, bgcolour = colour + (255,), colour + (0,)
        elif colour[-1] == 255: bgcolour = colour[:-1] + (0,)
        elif colour[-1] == 0: bgcolour = colour[:-1] + (255,)
        else: raise ValueError
    else:
        colour = (255, 255, 255, 0)
        bgcolour = (255, 255, 255, 255)
            
    img = _circle_image(radius, colour, bgcolour)
    io = StringIO()
    img.save(io, 'PNG')
    return io.getvalue()

@http('/pony/images/circle$radius-$colour.gif',           type='image/gif')
@http('/pony/images/circle$radius-$colour-$bgcolour.gif', type='image/gif')
@cached
def circle_gif(radius, colour, bgcolour='ffffff'):
    colour = _decode_colour(colour)
    if len(colour) != 3: raise ValueError
    bgcolour=_decode_colour(bgcolour)
    img = _circle_image(radius, colour, bgcolour)
    img = img.convert("P", dither=Image.NONE, palette=Image.ADAPTIVE)
    io = StringIO()
    img.save(io, 'GIF')
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
        pixels = img.load()
        for i in range(length):
            pixels[i, 0] = ((r1 + r2*i/length), (g1 + g2*i/length), (b1 + b2*i/length))
    if not horiz: img = img.rotate(270)
    if format == 'GIF': img = img = img.convert("P", dither=Image.NONE, palette=Image.ADAPTIVE)
    io = StringIO()
    img.save(io, format)
    return io.getvalue()

@http('/pony/images/hline$length-$colour.png',          type='image/png')
@http('/pony/images/hline$length-$colour-$colour2.png', type='image/png')
@cached
def hline_png(length, colour, colour2=None):
    return _line_image('PNG', True, length, colour, colour2)

@http('/pony/images/hline$length-$colour.gif',          type='image/png')
@http('/pony/images/hline$length-$colour-$colour2.gif', type='image/png')
@cached
def hline_gif(length, colour, colour2=None):
    return _line_image('GIF', True, length, colour, colour2)

@http('/pony/images/vline$length-$colour.png',          type='image/png')
@http('/pony/images/vline$length-$colour-$colour2.png', type='image/png')
@cached
def vline_png(length, colour, colour2=None):
    return _line_image('PNG', False, length, colour, colour2)

@http('/pony/images/vline$length-$colour.gif',          type='image/png')
@http('/pony/images/vline$length-$colour-$colour2.gif', type='image/png')
@cached
def vline_gif(length, colour, colour2=None):
    return _line_image('GIF', False, length, colour, colour2)
