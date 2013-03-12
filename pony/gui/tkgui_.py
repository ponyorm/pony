import pprint, threading, time
from datetime import timedelta
from operator import attrgetter
from Tkinter import *

import pony
from pony import utils
from pony.logging2 import search_log

UI_UPDATE_INTERVAL = 1000   # in ms
MAX_RECORD_DISPLAY_COUNT = 1000

class Grid(object):
    def __init__(self, parent, column_list, **params):
        self.parent = parent
        self.frame = Frame(parent, **params)
        self.listboxes = []
        for name, width, fixed in column_list:
            col_frame = Frame(self.frame)
            label = Label(col_frame, text=name, relief=RIDGE)
            listbox = Listbox(col_frame, width=width, height=18,
                              borderwidth=0, exportselection=FALSE)
            listbox.bind('<Button-1>', self.select)
            listbox.bind('<B1-Motion>', self.select)
            listbox.bind('<Leave>', lambda e: 'break')
            if fixed:
                col_frame.pack(side=LEFT, expand=N, fill=Y)
                label.pack(fill=X)
                listbox.pack(side=LEFT, expand=N, fill=Y)
            else:
                col_frame.pack(side=LEFT, expand=Y, fill=BOTH)
                label.pack(fill=X)
                listbox.pack(side=LEFT, expand=Y, fill=BOTH)
            self.listboxes.append(listbox)
        self.scrollbar = Scrollbar(self.frame, orient=VERTICAL,
                                   command=self.scroll)
        self.listboxes[0].config(yscrollcommand=self.scrollbar.set)
        self.scrollbar.pack(side=RIGHT, fill=Y)
        self.selected = -1
        self._show_last_record = False
    def scroll(self, *args):
        for listbox in self.listboxes: listbox.yview(*args)
    def select(self, e):
        row_num = self.listboxes[0].nearest(e.y)
        if row_num == self.selected: return 'break'
        for listbox in self.listboxes:
            listbox.select_clear(self.selected)
            listbox.select_set(row_num)
        self.selected = row_num
        self.on_select(row_num)
        return 'break'
    def on_select(self, row_num):
        pass
    def add(self, *args):
        assert len(args) == len(self.listboxes)
        for listbox, text in zip(self.listboxes, args):
            listbox.insert(END, text)
        if self._show_last_record: self.show(END)
    def clear(self):
        for listbox in self.listboxes: listbox.delete(0, END)
        self.selected = -1
    def _set_show_last_record(self, value):
        self._show_last_record = value
        if value: self.show(END)
    show_last_record = property(attrgetter('_show_last_record'),
                                _set_show_last_record)
    def show(self, record_num):
        for listbox in self.listboxes: listbox.see(record_num)

class TabSet(object):
    def __init__(self, parent, **params):
        self.parent = parent
        self.frame = Frame(parent, **params)
        self.top = Frame(self.frame, relief=GROOVE, borderwidth=1)
        self.top.pack(side=TOP, expand=N, fill=X)
        self.content = Frame(self.frame)
        self.content.pack(side=TOP, expand=Y, fill=BOTH)
        self.buttons = []
        self.tabs = []
        self.current_tab = None
        self.current_tab_number = IntVar(0)
    def add(self, tab_name):
        tab = Frame(self.content)
        tab_number = len(self.tabs)
        button = Radiobutton(self.top, text=tab_name, width=20, indicatoron=0,
                             variable=self.current_tab_number, value=tab_number,
                             command=lambda: self.display_tab(tab))
        button.pack(side=LEFT)
        self.tabs.append(tab)
        self.buttons.append(button)
        if not tab_number: self.display_tab(tab)
        return tab
    def display_tab(self, tab):
        if self.current_tab is not None: self.current_tab.forget()
        tab.pack(expand=Y, fill=BOTH)
        self.current_tab=tab

class TkMainWindow(Frame):
    def __init__(self, root):
        Frame.__init__(self, root)
        self.width = w = 1000
        self.height = h = 600
        self.root=root
        self.after_handler = None
        self.root.title('Pony')
        self.records=[]
        self.tab_buttons=[]

        screen_w = root.winfo_screenwidth()
        screen_h = root.winfo_screenheight()
        root.geometry("%dx%d%+d%+d" % (w, h, (screen_w-w)/2, (screen_h-h)/2))

        self.grid = Grid(self, [('timestamp', 20, True), ('type', 20, False)],
                         height=h/2)
        self.grid.frame.pack(side=TOP, fill=X)
        self.grid.show_last_record = True
        self.grid.on_select = self.show_record

        self.response_info = Label(self, text='', justify=LEFT,
                                   font='Helvetica 8 bold',
                                   wraplength=self.width-20)
        self.response_info.pack(side=TOP, anchor=W)

        self.tabs = TabSet(self, height=h/2, relief=GROOVE, borderwidth=2)
        self.tabs.frame.pack(side=TOP, expand=YES, fill=BOTH)

        request_tab = self.tabs.add("Request")
        self.request_headers = Grid(request_tab,
                                    [("HTTP request header", 20, False),
                                     ("value", 20, False)])
        self.request_headers.frame.pack(side=TOP, expand=YES, fill=BOTH)

        response_tab = self.tabs.add("Response")
        self.response_headers = Grid(response_tab,
                                     [("HTTP response header", 20, False),
                                      ("value", 20, False)])
        self.response_headers.frame.pack(side=TOP, expand=YES, fill=BOTH)

        exceptions_tab = self.tabs.add("Exceptions")
        self.exceptions_field = Text(exceptions_tab)
        self.exceptions_field.pack(expand=Y, fill=BOTH)

        session_tab = self.tabs.add("Session")
        self.session_field = Text(session_tab)
        self.session_field.pack(expand=Y, fill=BOTH)

        self.makemenu()
        self.pack(expand=YES, fill=BOTH)

    def load(self, since_last_start=True):
        start_id = 0
        if since_last_start:
            start = search_log(1, None, "type='HTTP:start'")
            if start: start_id = start[0]['id']
        data = search_log(MAX_RECORD_DISPLAY_COUNT, None,
            "type like 'HTTP:%' and type <> 'HTTP:response' and id >= ?",
            [ start_id ])
        data.reverse()
        self.add_records(data)

    def check_data(self):
        try:
            last_id = self.records[-1].data['id']
            data = search_log(-1000, last_id,
                              "type like 'HTTP:%' and type <> 'HTTP:response'")
            self.add_records(data)
        finally:
            self.after_handler = self.after(UI_UPDATE_INTERVAL, self.check_data)

    def add_records(self, records):
        for r in records:
            rtype = r['type']
            if rtype == 'HTTP:start': record = HttpStartRecord(r)
            elif rtype == 'HTTP:stop': record = HttpStopRecord(r)
            else:record = HttpRequestRecord(r)
            self.records.append(record)
            self.grid.add(r['timestamp'][:-7], record.get_text())

    def makemenu(self):
        top=Menu(self.root)
        self.root.config(menu=top)
        options=Menu(top, tearoff=0)
        var = self.show_last_record_var = IntVar()
        var.set(1)
        options.add('checkbutton', label='Show Last Record',
                    variable=var, command=self.show_last_record)
        var = self.show_since_start_var = IntVar()
        var.set(1)
        options.add('checkbutton', label='Show Since Last Start Only',
                    variable=var, command=self.show_since_start)
        top.add_cascade(label='Options', menu=options)

    def show_last_record(self):
        self.grid.show_last_record = self.show_last_record_var.get()

    def show_since_start(self):
        self.clear_tabs()
        self.grid.clear()
        self.records = []
        x = self.show_since_start_var.get()
        self.load(x)

    def clear_tabs(self):
        self.response_info.config(text='')
        self.request_headers.clear()
        self.response_headers.clear()
        self.exceptions_field.delete(1.0, END)
        self.session_field.delete(1.0, END)

    def show_record(self, rec_no):
        rec=self.records[rec_no]
        self.clear_tabs()
        rec.draw(self)

class Record(object):
    def __init__(self, data):
        self.data = data
    def get_text(self):
        return self.data['text']
    def draw(self, widget):
        process_id = self.data['process_id']
        thread_id = self.data['thread_id']
        record_id = self.data['id']
        text = ('PROCESS_ID: %d; THREAD_ID: %d; RECORD_ID: %d'
                % (process_id, thread_id, record_id))
        widget.response_info.config(text=text)

class HttpStartRecord(Record): pass
class HttpStopRecord(Record): pass

class HttpRequestRecord(Record):
    def get_text(self):
        return '%s %s' % (self.data['headers'].get('REQUEST_METHOD', 'GET'),
                          self.data['text'])
    def draw(self, widget):
        data = self.data
        for k, v in sorted(data['headers'].items()):
            widget.request_headers.add(k, v)

        session_text = pprint.pformat(data['session'])
        widget.session_field.insert(END, session_text)

        process_id = self.data['process_id']
        thread_id = self.data['thread_id']
        record_id = self.data['id']
        records = search_log(-1, record_id,
            "type like 'HTTP:%' and process_id = ? and thread_id = ?",
            [ process_id, thread_id ])
        if records and records[0]['type'] == 'HTTP:response':
            resp = records[0]
            dt1 = utils.timestamp2datetime(data['timestamp'])
            dt2 = utils.timestamp2datetime(resp['timestamp'])
            delta = dt2 - dt1
            delta=delta.seconds + 0.000001 * delta.microseconds
            text = ("STATUS: %s; DELAY: %s; "
                    "PROCESS_ID: %d; THREAD_ID: %d; RECORD_ID: %d" %
                   (resp['text'], delta, process_id, thread_id, record_id))
            if data["user"] is not None: text += "; USER: %s" % data["user"]
            exceptions = search_log(-10, record_id,
                                    "type = 'exception' and id < ? "
                                    "and process_id = ? and thread_id = ?",
                                    [ resp['id'], process_id, thread_id ])
            if exceptions: text += "; EXCEPTION: " + exceptions[0]['text']
            widget.response_info.config(text=text)
            for k, v in sorted(resp['headers']):
                widget.response_headers.add(k, v)
            exc_text = []
            for exc in exceptions:
                exc_text.append(exc['traceback'])
            widget.exceptions_field.insert(END, '\n'.join(exc_text))
        else: Record.draw(self)

tk_thread = None
tk_lock = threading.Lock()

class TkThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.setDaemon(True)
    def quit(self, event):
        self.window.after_cancel(self.window.after_handler)
        self.window.destroy()
        self.root.quit()
        self.window.tabs.current_tab_number = None
        self.window.show_last_record_var = None
        self.window.show_since_start_var = None
    def run(self):
        global tk_thread
        time.sleep(.5)
        tk_lock.acquire()
        try:
            if tk_thread: return
            tk_thread = self
        finally: tk_lock.release()
        try:
            self.root = Tk()
            self.window = TkMainWindow(self.root)
            self.window.bind("<Destroy>", self.quit)
            self.window.load()
            self.window.check_data()
            self.root.mainloop()
        finally:
            tk_lock.acquire()
            try:
                assert tk_thread is self
                tk_thread = None
            finally: tk_lock.release()

def show_gui():
    if tk_thread: return
    TkThread().start()

@pony.on_shutdown
def do_shutdown():
    if tk_thread is not None: tk_thread.quit(None)
