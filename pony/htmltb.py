import sys, cgitb, cStringIO

def format_exc():
    exc_type, exc_value, traceback = sys.exc_info()
    try:
        io = cStringIO.StringIO()
        hook = cgitb.Hook(file=io)
        hook.handle((exc_type, exc_value, traceback))
        return io.getvalue()
    finally: del traceback
