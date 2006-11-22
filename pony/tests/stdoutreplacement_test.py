from pony.stdoutreplacement import grab_stdout

@grab_stdout
def f(a, b):
    print a, '+', b, '=', a + b
    print 123
    return 0

print f(3, 7)

raw_input()