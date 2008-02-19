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

def _normalize_colour(colour):
    try: return ImageColor.colormap[colour][1:]
    except KeyError: pass
    size = len(colour)
    if size in (3, 4): return ''.join(char+char for char in colour)
    elif size in (6, 8): return colour
    raise ValueError

def _circle_image(radius, colour, bgcolour):
    if not PIL: raise ValueError
    if not 0 <= radius <= 100: raise ValueError
    try:
        colour = tuple(map(ord, unhexlify(colour)))
        bgcolour = tuple(map(ord, unhexlify(bgcolour)))
    except: raise ValueError
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

@http('/pony/images/circle$radius.png', type='image/png')
@http('/pony/images/circle$radius-$colour.png', type='image/png')
@http('/pony/images/circle$radius-$colour-$bgcolour.png', type='image/png')
@cached
def circle_png(radius, colour=None, bgcolour=None):
    try: radius = int(radius)
    except: raise ValueError

    if colour is not None:
        colour = _normalize_colour(colour)
        if bgcolour is not None: bgcolour = _normalize_colour(bgcolour)
        elif len(colour) == 6: colour, bgcolour = colour + 'ff', colour + '00'
        elif colour[-2:] == 'ff': bgcolour = colour[:-2] + '00'
        elif colour[-2:] == '00': bgcolour = colour[:-2] + 'ff'
        else: raise ValueError
    else: colour, bgcolour = 'ffffff00', 'ffffffff'
            
    im = _circle_image(radius, colour, bgcolour)
    io = StringIO()
    im.save(io, 'PNG')
    return io.getvalue()

@http('/pony/images/circle$radius-$colour.gif', type='image/gif')
@http('/pony/images/circle$radius-$colour-$bgcolour.gif', type='image/gif')
@cached
def circle_gif(radius, colour, bgcolour='ffffff'):
    try: radius = int(radius)
    except: raise ValueError

    colour = _normalize_colour(colour)
    if len(colour) != 6: raise ValueError
    bgcolour=_normalize_colour(bgcolour)
    
    im = _circle_image(radius, colour, bgcolour)
    im = im.convert("P", dither=Image.NONE, palette=Image.ADAPTIVE)
    io = StringIO()
    im.save(io, 'GIF')
    return io.getvalue()
