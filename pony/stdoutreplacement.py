import sys, threading

def push(writer):
    local.writers.append(writer)

def pop():
    writers = local.writers
    assert len(writers) > 1
    return writers.pop()

def grab_stdout(f):
    def new_function(*args, **keyargs):
        data = []
        push(data.append)
        try: data.append(f(*args, **keyargs))
        finally: assert pop() == data.append
        return data
    new_function.__name__ = f.__name__
    new_function.__doc__ = f.__doc__
    return new_function

################################################################################

old_stdout = sys.stdout

class Local(threading.local):
    def __init__(self):
        self.writers = [ old_stdout.write ]

local = Local()

class ThreadedStdout(object):
    @staticmethod
    def write(s):
        local.writers[-1](s)

sys.stdout = ThreadedStdout()
