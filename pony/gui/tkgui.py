import pony

def show_gui():
    if pony.MODE not in ('INTERACTIVE', 'CHERRYPY'): return
    from pony.gui.tkgui_ import show_gui
    show_gui()
    