from __future__ import division

import time, threading, os.path, glob
from math import sin, cos, sqrt, exp
from random import randint, random, sample, choice

try: import Image, ImageDraw, ImageFont, ImageChops
except ImportError: raise ImportError('Cannot import pony.capthca because PIL does not installed')

import pony

if pony.MODE.startswith('GAE-'): raise ImportError('Module pony.capthca does not work inside GAE')

dir = os.path.dirname(__file__)
main = __name__ == '__main__'

numbers = '123456789'
vovels = 'aeiou'
consonants = 'bsdfghjklmnpqrstvwxz'

def generate_text():
    letter_pairs_count = choice([3, 4])
    s = ''.join(choice(consonants) + choice(vovels) for i in range(letter_pairs_count))
    if letter_pairs_count == 3: s += ''.join(sample(numbers, 2))
    elif randint(0, 1): s = s[:-1]
    return s

font_fnames = glob.glob(os.path.join(dir, '*.ttf'))

fonts = [ ImageFont.truetype(fname, 60) for fname in font_fnames ]

def get_letter_glyph(letter, font):
    text = ' ' + letter + ' '
    size = font.getsize(text)
    img = Image.new(MODE, size, BLACK)
    draw = ImageDraw.ImageDraw(img)
    draw.text((0, 0), text, fill=WHITE, font=font)
    x1, y1, x2, y2 = img.getbbox()
    return img.crop((x1, 0, x2, size[1]))

maps = []

if __name__ != '__main__':
    fname = os.path.join(dir, 'map.dat')
    map = file(fname, 'rb').read().decode('zip')
    maps.append(map)

QUALITY = 3

MODE = 'L'
BLACK = 0
WHITE = 255

WIDTH = 320
HEIGHT = 110

TEXT_X = 15
TEXT_Y = 30

def generate_captcha():
    text = generate_text()
    font = choice(fonts)
    
    img = Image.new(MODE, (WIDTH, HEIGHT), BLACK)
    pix = img.load()
    draw = ImageDraw.ImageDraw(img)

    text_width, text_height = draw.textsize(text, font=font)
    space_width = draw.textsize(' ', font=font)[0]
    text_offset = TEXT_X
    for i in range(len(text)):
        glyph = get_letter_glyph(text[i], font)
        img.paste(WHITE, (text_offset, randint(0, 15)), glyph)
        text_offset += glyph.size[0] - randint(4, 6)

    if maps: 
        map = choice(maps)
        next = iter(map).next

        img2 = Image.new(MODE, (WIDTH, HEIGHT), BLACK)
        pix2 = img2.load()
        xrange = range(WIDTH)
        qrange = range(QUALITY**2)
        for y in range(HEIGHT):
            for x in xrange:
                color_sum = BLACK
                for q in qrange:
                    xchar = next()
                    ychar = next()
                    if xchar == '\x00' == ychar: continue
                    x2 = x + ord(xchar) - 128
                    y2 = y + ord(ychar) - 128
                    color_sum += pix[x2, y2]
                pix2[x, y] = int(color_sum/QUALITY**2)
        img = img2

    x1, y1, x2, y2 = img.getbbox()
    img = img.crop((x1-2, y1-2, x2+2, y2+2))
    return text, img

def generate_map():
    X_SCALE = randint(15, 25)
    Y_SCALE = randint(15, 25)
    X_AMP = randint(3, 6)
    Y_AMP = randint(3, 6)
    X_PHASE = 6.28*random()
    Y_PHASE = 6.28*random()
    
    SWIRL_X = randint(60, 100)
    SWIRL_Y = randint(70, 80)
    SWIRL_ANGLE = (random() - 0.5)
    if SWIRL_ANGLE > 0: SWIRL_ANGLE += 0.4
    else: SWIRL_ANGLE -= 0.4
    
    SWIRL2_X = randint(120, 160)
    SWIRL2_Y = randint(80, 90)
    SWIRL2_ANGLE = (random() - 0.5)
    if SWIRL2_ANGLE > 0: SWIRL2_ANGLE += 0.2
    else: SWIRL2_ANGLE -= 0.2

    qrange = range(QUALITY)
    ylist = [   (Y, [   (y, sin(y/Y_SCALE + Y_PHASE)*Y_AMP)
                        for y in (Y + dy/QUALITY for dy in qrange)   ])
                for Y in range(HEIGHT)   ]
    xlist = [   (X, [   (x, sin(x/X_SCALE + X_PHASE)*X_AMP)
                        for x in (X + dx/QUALITY for dx in qrange)   ])
                for X in range(WIDTH)   ]

    map = []
    
    for Y, ylist2 in ylist:
        if pony.shutdown: return
        if not (Y % 10) and not main: time.sleep(.1)
        for X, xlist2 in xlist:
            for y, xshift in ylist2:
                for x, yshift in xlist2:
                    x2 = x + xshift
                    y2 = y + yshift

                    delta_x = x2 - SWIRL_X
                    delta_y = y2 - SWIRL_Y
                    radius = sqrt(delta_x**2 + delta_y**2)
                    angle = SWIRL_ANGLE * exp(-(radius/30)**2)
                    c = cos(angle)
                    s = sin(angle)
 
                    x3 = SWIRL_X + delta_x*c - delta_y*s
                    y3 = SWIRL_Y + delta_x*s + delta_y*c

                    delta_x = x3 - SWIRL2_X
                    delta_y = y3 - SWIRL2_Y
                    radius = sqrt(delta_x**2 + delta_y**2)
                    angle = SWIRL2_ANGLE * exp(-(radius/30)**2)
                    c = cos(angle)
                    s = sin(angle)

                    x4 = SWIRL2_X + delta_x*c - delta_y*s
                    y4 = SWIRL2_Y + delta_x*s + delta_y*c

                    x_result = int(x4)
                    y_result = int(y4)
                    
                    if x_result < 0 or x_result >= WIDTH or y_result < 0 or y_result >= HEIGHT:
                        map.append('\x00\x00')
                    else:
                        map.append(chr(x_result - X + 128))
                        map.append(chr(y_result - Y + 128))
    return ''.join(map)

class CaptchaThread(threading.Thread):
    def __init__(captcha_thread):
        threading.Thread.__init__(captcha_thread, name="CaptchaThread")
        captcha_thread.setDaemon(True)
    def run(captcha_thread):
        if not main: time.sleep(5)
        for i in range(10):
            map = generate_map()
            if pony.shutdown: return
            maps.append(map)
            if main: print i+1
            # fname = os.path.join(dir, 'map-%d' % (i+1))
            # file(fname, 'wb').write(map.encode('zip'))
        maps.pop(0)

captcha_thread = CaptchaThread()
captcha_thread.start()

if __name__ == '__main__':
    captcha_thread.join()
    for i in range(20):
        print '.',
        text, img = generate_captcha()
        img.save('captcha-%02d-%s.jpg' % (i+1, text))
