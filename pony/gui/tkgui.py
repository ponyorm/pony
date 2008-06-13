import pony

def show_gui():
    if pony.RUNNED_AS not in ('INTERACTIVE', 'NATIVE'): return
    import pony.gui.tkgui_
    pony.gui.tkgui_.show_gui()
    