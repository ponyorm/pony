from Tkinter import *
from pony.logging import search_log
from pony import utils
from datetime import timedelta
from operator import attrgetter
import threading, time, Queue

UI_UPDATE_INTERVAL=250   # in ms
DB_UPDATE_INTERVAL=1     # in sec

class Grid(object):
    def __init__(self, parent, column_list, **params):
        self.parent = parent
        self.frame = Frame(parent, **params)
        self.listboxes = []
        for name, width in column_list:
            col_frame = Frame(self.frame)
            label = Label(col_frame, text=name, relief=RIDGE)
            listbox = Listbox(col_frame, width=width, height=18,
                              borderwidth=0, exportselection=FALSE)
            listbox.bind('<Button-1>', self.select)
            listbox.bind('<B1-Motion>', self.select)
            listbox.bind('<Leave>', lambda e: 'break')
            if name == 'timestamp':
                col_frame.pack(side=LEFT, expand=N, fill=Y)
                label.pack(fill=X)
                listbox.pack(side=LEFT, expand=Y, fill=Y)
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
    def add_row(self, *args):
        assert len(args) == len(self.listboxes)
        for listbox, text in zip(self.listboxes, args):
            listbox.insert(END, text)
        if self._show_last_record:
            for listbox in self.listboxes: listbox.see(END)
    def _set_show_last_record(self, value):
        self._show_last_record = value
        if value:
            for listbox in self.listboxes: listbox.see(END)
    show_last_record = property(attrgetter('_show_last_record'),
                                _set_show_last_record)

class ViewerWidget(Frame):
    def __init__(self, root): 
        Frame.__init__(self, root)
        self.data_queue=Queue.Queue()
        self.width = w = 1000
        self.height = h = 600        
        self.root=root
        self.root.title('Pony')
        self.records=[]       
        self.headers={} 
        self.tab_buttons=[]

        self.current_tab = None
        self.current_tab_n = IntVar(0)
        self.tab_count = 0

        screen_w = root.winfo_screenwidth()
        screen_h = root.winfo_screenheight()
        root.geometry("%dx%d%+d%+d" % (w, h, (screen_w-w)/2, (screen_h-h)/2))

        self.grid = Grid(self, [('timestamp', 20), ('type', 20)], height=h/2)
        self.grid.frame.pack(side=TOP, fill=X)
        self.grid.show_last_record = True
        self.grid.on_select = self.show_record
          
        self.down=Frame(self, height=h/2, relief=GROOVE, borderwidth=2)
        self.down.pack(side=TOP, expand=YES, fill=BOTH)        
        self.create_tabs(self.down)
        self.makemenu()
        self.pack(expand=YES, fill=BOTH)
        self.ds=DataSupplier(self)
        self.feed_data()
        self.ds.start()

    def makemenu(self):
        top=Menu(self.root)
        self.root.config(menu=top)
        options=Menu(top, tearoff=0)
        self.showLastRecord=IntVar()
        self.showLastRecord.set(1)
        options.add('checkbutton', label='Show Last Record', variable=self.showLastRecord,
                    command=self.show_last_record)
        self.showSinceStart=IntVar()
        self.showSinceStart.set(1)
        options.add('checkbutton', label='Show Since Last Start Only', variable=self.showSinceStart,
                    command=self.show_since_start)
        top.add_cascade(label='Options', menu=options)

    def show_last_record(self):
        self.grid.show_last_record = self.showLastRecord.get()

    def show_since_start(self):
        self.clear_tabs()
        self.main_col_list[0].delete(0, END)
        self.main_col_list[1].delete(0, END)
        self.ds.stop()
        self.ds=DataSupplier(self, loadLastOnly=self.showSinceStart.get())
        #self.ds.loadLastOnly=self.showSinceStart.get()
        self.ds.start()

    def _create_tab_blank(self, tab_name, tab_number):
        tab_field=Frame(self.tab_body_field)       
        tab_button = Radiobutton(self.tab_buttons_field, text=tab_name, width=20, indicatoron=0,
                                 variable=self.current_tab_n , value=tab_number,
                                 command=lambda tab_field=tab_field: self.display_tab(tab_field))
        tab_button.pack(side=LEFT)
        self.tab_buttons.append(tab_button)
        return tab_field

    def create_summary_tab(self):
        summary_tab=self._create_tab_blank("summary", 0) 
        summary_frame=Frame(summary_tab)
        summary_frame.pack(expand=Y, fill=BOTH)
        self.summary_field=Text(summary_tab)
        self.summary_field.pack(expand=Y, fill=BOTH)
        return summary_tab 

    def create_request_tab(self):
        def scroll_hdrs(*args):
            self.req_lb1.yview(*args)
            self.req_lb2.yview(*args)

        def select_row(e):
            row = self.req_lb1.nearest(e.y)
            for lb in (self.req_lb1, self.req_lb2):
                lb.select_clear(0,END)
                lb.select_set(row)
            return 'break'

        request_tab=self._create_tab_blank("request", 1) 
        headers_frame=Frame(request_tab)
        headers_frame.pack(expand=Y, fill=BOTH)
        self.req_lb1=Listbox(headers_frame, borderwidth=0, width=10, exportselection=FALSE)
        self.req_lb1.pack(side=LEFT, expand=Y, fill=BOTH) 
        self.req_lb2=Listbox(headers_frame, borderwidth=0, width=90, exportselection=FALSE)
        self.req_lb2.pack(side=LEFT, expand=Y, fill=BOTH)
        for lb in (self.req_lb1, self.req_lb2):
            lb.bind('<Button-1>', select_row)
            lb.bind('<B1-Motion>', select_row)          
            lb.bind('<Leave>', lambda e: 'break')      
        sb=Scrollbar(headers_frame, orient=VERTICAL, command=scroll_hdrs)
        self.req_lb1.config(yscrollcommand=sb.set)
        sb.pack(side=RIGHT, fill=Y)
        return request_tab

    def create_response_tab(self): 
        def scroll_hdrs(*args):
            self.res_lb1.yview(*args)
            self.res_lb2.yview(*args)

        def select_row(e):
            row = self.res_lb1.nearest(e.y)
            for lb in (self.res_lb1, self.res_lb2):
                lb.select_clear(0,END)
                lb.select_set(row)
            return 'break'
       
        response_tab=self._create_tab_blank("response", 2) 
        self.response_info=Label(response_tab, text='', justify=LEFT,
                           font='Helvetica 8 bold', wraplength= self.width-20)
        self.response_info.pack(side=TOP, anchor=W) 
        headers_frame=Frame(response_tab)
        headers_frame.pack(expand=Y, fill=BOTH)
        self.res_lb1=Listbox(headers_frame, borderwidth=0, width=10, exportselection=FALSE)
        self.res_lb1.pack(side=LEFT, expand=Y, fill=BOTH) 
        self.res_lb2=Listbox(headers_frame, borderwidth=0, width=90, exportselection=FALSE)
        self.res_lb2.pack(side=LEFT, expand=Y, fill=BOTH)
        for lb in (self.res_lb1, self.res_lb2):
            lb.bind('<Button-1>', select_row)
            lb.bind('<B1-Motion>', select_row)          
            lb.bind('<Leave>', lambda e: 'break')      
        sb=Scrollbar(headers_frame, orient=VERTICAL, command=scroll_hdrs)
        self.req_lb1.config(yscrollcommand=sb.set)
        sb.pack(side=RIGHT, fill=Y)


    def create_tabs(self, parent):  
        self.tab_buttons_field=Frame(parent, relief=GROOVE, borderwidth=1)
        self.tab_buttons_field.pack(side=TOP, expand=N, fill=X)
       
        self.tab_body_field=Frame(parent)
        self.tab_body_field.pack(side=TOP, expand=Y, fill=BOTH)

        # self.current_tab=self.create_summary_tab()
        self.current_tab = self.create_request_tab()
        self.create_response_tab()
        self.current_tab.pack(expand=Y, fill=BOTH) #show the first tab          

    def display_tab(self, tab):
        self.current_tab.forget()
        tab.pack(expand=Y, fill=BOTH)
        self.current_tab=tab

    def add_record(self, rec):       
        self.records.append(rec)
        self.grid.add_row(rec.data['timestamp'][:-7], rec.data['text'])

    def clear_tabs(self):
        # self.summary_field.delete(1.0, END)
        self.response_info.config(text='')
        for lb in (self.req_lb1 , self.req_lb2, self.res_lb1, self.res_lb2):
            lb.delete(0, END)

    def show_record(self, rec_no):
        rec=self.records[rec_no]
        self.clear_tabs()      
        rec.draw(self)

    def feed_data(self):        
        while True:
            try:
                rec=self.data_queue.get(block=False)
            except Queue.Empty:
                self.after(UI_UPDATE_INTERVAL, self.feed_data)
                break                
            else:                
                self.add_record(rec)

    def stop_data_supply(self):
        self.ds.stop()


class Record(object):
    def __init__(self, data):
        self.data = data

class StartRecord(Record):
    def draw(self, widget):
        for i, button in enumerate(widget.tab_buttons ):
            if i == 0:
                button.select()
                button.invoke()
            else: button.forget()              
        # data = self.data
        # txt='TIMESTAMP: \t%s\nTEXT: \t\t%s' % (data['timestamp'], data['text'])
        # widget.summary_field.insert(END, txt)

class RequestRecord(Record): 
    def draw(self, widget):
        data = self.data
        for i, button in enumerate(widget.tab_buttons):
            if i == 0:
                pass
                #button.select() 
                #button.invoke()
            else: button.pack(side=LEFT)
        # widget.summary_field.insert(END, "some summary text")
        for k, v in data['headers'].items():
            widget.req_lb1.insert(END, k)
            widget.req_lb2.insert(END, v)
        crit='process_id=%i and thread_id=%i' % (data['process_id'], data['thread_id'])
        rows=search_log(criteria=crit, start_from=data['id'], max_count=-1)
        for rec in rows:
            if rec['type'] == 'HTTP:response':
                dt1 = utils.timestamp2datetime(data['timestamp'])
                dt2 = utils.timestamp2datetime(rec['timestamp'])
                delta = dt2 - dt1
                delta=delta.seconds + 0.000001*delta.microseconds
                
                txt='TEXT: \t%s %s\nDELAY: \t%s' % (rec['type'], rec['text'], delta)
                widget.response_info.config(text=txt)
                for k, v in rec['headers']:
                    widget.res_lb1.insert(END, k)
                    widget.res_lb2.insert(END, v)
                #break

class DataSupplier(threading.Thread):
    def __init__(self, vw, loadLastOnly=1):        
        threading.Thread.__init__(self)
        self.loadLastOnly=loadLastOnly
        self.setDaemon(True)
        self.keepRunning=True
        self.vw=vw        
        self.last_record_id=-1
        self._max_count=100
        
    def load_data(self, _start_from):
        rows=search_log(start_from=_start_from, max_count=-self._max_count)
        for rec in rows:            
            self.last_record_id=rec['id']
            rec_type=rec['type']
            if rec_type in ('HTTP:GET', 'HTTP:POST'): record=RequestRecord(rec) 
            elif rec_type == 'HTTP:start': record=StartRecord(rec) 
            else: continue
            self.vw.data_queue.put(record)
        return len(rows) == self._max_count

    def stop(self):
        self.keepRunning=False

    def get_last_start_id(self): 
        _start_from=None
        while True:
            rows=search_log(max_count=10, start_from=_start_from)
            if len(rows) == 0: return 0
            for rec in rows:
                if rec['type'] == 'HTTP:start': 
                    return int(rec['id'])-1
                _start_from=rec['id']
   
    def run(self):        
        if self.loadLastOnly == 1:
            self.last_record_id=self.get_last_start_id()
        else:
            self.last_record_id=1
        while self.load_data(self.last_record_id):
            pass # load all data
        while self.keepRunning:
            self.load_data(self.last_record_id )
            time.sleep(DB_UPDATE_INTERVAL)

class WidgetRunner(threading.Thread):
    def run(self):
        root=Tk()
        vw=ViewerWidget(root)
        vw.feed_data()        
        root.mainloop()
        vw.stop_data_supply()

def show_gui():
    wr=WidgetRunner()
    wr.start()
