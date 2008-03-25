from _pony_templating import Html
from timeit import Timer
x = Html('<a> &" %d')
y = Html('<b> %s %d %s')
z = Html('<b>')
t1 = Timer("'f' + (x % 2) * 3 + 2 * (y %('a', 1, '<&>')) + 'w' + z.join(['a', '<&>']) ", 'from __main__ import x, y, z')

from _pony_templating import StrHtml
x = StrHtml('<a> &" %d')
y = StrHtml('<b> %s %d %s')
z = StrHtml('<b>')
t2 = Timer("'f' + (x % 2) * 3 + 2 * (y %('a', 1, '<&>')) + 'w' + z.join(['a', '<&>']) ", 'from __main__ import x, y, z')
