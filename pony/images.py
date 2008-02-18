from cStringIO import StringIO
from binascii import unhexlify

try: import Image, ImageDraw
except ImportError: PIL = False
else: PIL = True

from pony.web import http

def _decode_colour(colour):
    size = len(colour)
    if size not in (3, 4, 6, 8): raise ValueError
    if size < 6: colour = ''.join(a+a for a in colour)
    return tuple(map(ord, unhexlify(colour)))

def _circle_image(radius, colour, bgcolour):
    if not PIL: raise ValueError
    if not 0 <= radius <= 100: raise ValueError
    colour = _decode_colour(colour)
    bgcolour = _decode_colour(bgcolour)
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

@http('/pony/images/circle$radius-$colour-$bgcolour.png', type='image/png')
def circle_png(radius, colour, bgcolour):
    try: radius = int(radius)
    except: raise ValueError
    im = _circle_image(radius, colour, bgcolour)
    io = StringIO()
    im.save(io, 'PNG')
    return io.getvalue()

@http('/pony/images/circle$radius-$colour-$bgcolour.gif', type='image/gif')
def circle_gif(radius, colour, bgcolour):
    bgcolour='eee'
    try: radius = int(radius)
    except: raise ValueError
    if len(colour) not in (3, 6): raise ValueError
    im = _circle_image(radius, colour, bgcolour)
    io = StringIO()
    im.save(io, 'GIF')
    return io.getvalue()
