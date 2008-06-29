import pony

def show_gui():
    if pony.RUNNED_AS not in ('INTERACTIVE', 'NATIVE'): return
    from pony.gui.tkgui_ import show_gui
    show_gui()
    