from Tkinter import *

class PonyGUI:

    def __init__(self):
        self.root = Tk()
        w = 800
        h = 600
        ws = self.root.winfo_screenwidth()
        hs = self.root.winfo_screenheight()

        self.root.geometry("%dx%d%+d%+d" % (w, h, (ws/2) - (w/2), (hs/2) - (h/2)))
        self.tab_count = 0
        self.current_tab = None
        self.main_frame = Frame(self.root)
        self.main_frame.pack(expand=YES, fill=BOTH)
        
        self.status_frame = Frame(self.main_frame, relief=RIDGE)
        self.status_frame.pack(side=BOTTOM, expand=Y, fill=BOTH)
        self.status_text = Text(self.status_frame, background="GREY", height=8)
        self.status_text.pack(side=BOTTOM, expand=Y, fill=BOTH)
        
        self.button_frame = Frame(self.main_frame, relief=RIDGE)
        self.button_frame.pack(side=RIGHT, fill=BOTH, padx=4)
        
        self.tab_frame = Frame(self.main_frame, relief=RIDGE)
        self.tab_frame.pack(expand=Y, fill=BOTH)

        self.current_tab_n = IntVar(0)
        self.tabs = {}

    def add_tab(self, tittle):
        tab = Text(self.tab_frame)
                
        button = Radiobutton(self.button_frame, text=tittle, indicatoron=0, \
                             variable=self.current_tab_n, value=self.tab_count, \
                             command=lambda:self.display(tab))
        self.tab_count += 1
        button.pack(side=TOP, fill=X)
        if self.tabs.setdefault(tittle, tab) != tab:
            raise Exception("Duplicate tab tittle: %s" % tittle)
        
        # if it is first tab - display it
        if not self.current_tab:
            self.current_tab = tab
            tab.pack(expand=Y,fill=BOTH)

    def display(self, tab):
        self.current_tab.forget()
        tab.pack(expand=Y, fill=BOTH)
        self.current_tab = tab

    def log_to_tab(self, tittle, text):
        tab = self.tabs.get(tittle)
        if tab is None:
            raise Exception("No tab with such tittle: %s" % tittle)
        tab.insert(END, text)

    def log_to_status(self, text):
        self.status_text.insert(END, text)
    
    def mainloop(self):
        self.root.mainloop()


if __name__ == "__main__":
    pg = PonyGUI()
    pg.add_tab('localhost:8080')
    pg.add_tab('localhost:8081')
    pg.log_to_tab("localhost:8080", "start server at localhost:8080")
    pg.log_to_tab("localhost:8081", "start server at localhost:8081")
    pg.log_to_status("server was started")
    pg.mainloop()