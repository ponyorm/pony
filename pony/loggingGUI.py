from Tkinter import *
from pony.logging import search_log
from datetime import timedelta
import threading, time, Queue

UI_UPDATE_INTERVAL=250   # in ms
DB_UPDATE_INTERVAL=1     # in sec

class ViewerWidget(Frame):
    def __init__(self, root): 
        Frame.__init__(self, root)
        self.data_queue=Queue.Queue()
        self.w = w = 1000
        self.h = h = 600        
        self.root=root
        self.root.title('Pony')
        self.records=[]       
        self.headers={} 
        self.tab_buttons=[]

        self.current_tab = None
        self.current_tab_n = IntVar(0)
        self.tab_count = 0

        ws = root.winfo_screenwidth ()
        hs = root.winfo_screenheight ()
        root.geometry("%dx%d%+d%+d" % (w, h, (ws/2) - (w/2), (hs/2) - (h/2)))
        self.main_col_list = []
        self.last_selected = -1
        
        self.up=Frame (self, height=h//2)
        self.up.pack (side=TOP, fill=X)
        self.create_main(self.up)
          
        self.down=Frame(self, height=h//2, relief=GROOVE, borderwidth=2)
        self.down.pack(side=TOP, expand=YES, fill=BOTH)        
        self.create_tabs( self.down)
        self.makemenu()
        self.pack(expand=YES, fill=BOTH)
        self.ds=DataSupplier(self)
        self.feed_data()
        self.ds.start()

    def makemenu(self):
        top=Menu(self.root)
        self.root.config(menu=top)
        options=Menu(top, tearoff=0)
        self.showLastRecord=IntVar(value=1)
        options.add('checkbutton', label='Show Last Record', variable=self.showLastRecord, \
                    command=lambda: self.show_last_record())
        self.showSinceStart=IntVar(value=1)
        options.add('checkbutton', label='Show Since Last Start Only', variable=self.showSinceStart, \
                    command=lambda: self.show_since_start())
        top.add_cascade(label='Options', menu=options)

    def create_main(self, parent):
        def _scroll(*args):
            for col in self.main_col_list:
                apply( col.yview, args)
        def _select(e):
            row = self.main_col_list [0].nearest(e.y)
            if row==self.last_selected: return 'break'
            for col in self.main_col_list:
                col.select_clear(self.last_selected)
                col.select_set(row)
            self.last_selected=row
            self.show_record(row)
            return 'break'
        main_columns=[('timestamp', 20), ('type', 20)] 
        for name, w in main_columns:          
            cellframe = Frame(parent)            
            la = Label(cellframe, text=name, relief=RIDGE)            
            lb = Listbox(cellframe, borderwidth=0, width=w, height=18, exportselection=FALSE)         
            lb.bind('<Button-1>', lambda e: _select(e))
            lb.bind('<B1-Motion>', lambda e: _select(e))          
            lb.bind('<Leave>', lambda e: 'break')
            if name=='timestamp':
                cellframe.pack(side=LEFT, expand=N, fill=Y)
                la.pack(fill=X)
                lb.pack(side=LEFT, expand=Y, fill=Y)                
            else:
                cellframe.pack(side=LEFT, expand=Y, fill=BOTH)
                la.pack(fill=X)
                lb.pack(side=LEFT, expand=Y, fill=BOTH)                
            self.main_col_list.append(lb)
        sb=Scrollbar(parent, orient=VERTICAL, command=_scroll) 
        self.main_col_list[0].config(yscrollcommand=sb.set)
        sb.pack(side=RIGHT, fill=Y)


    def _create_tab_blank(self, tab_name, tab_number):
        tab_field=Frame(self.tab_body_field)       
        tab_button = Radiobutton(self.tab_buttons_field, text=tab_name, width=20, indicatoron=0, \
                                 variable=self.current_tab_n , value=tab_number, \
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
            apply(self.req_lb1.yview, args)
            apply(self.req_lb2.yview, args)

        def select(e):
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
            lb.bind('<Button-1>', lambda e: select(e))
            lb.bind('<B1-Motion>', lambda e: select(e))          
            lb.bind('<Leave>', lambda e: 'break')      
        sb=Scrollbar(headers_frame, orient=VERTICAL, command=scroll_hdrs)
        self.req_lb1.config(yscrollcommand=sb.set)
        sb.pack(side=RIGHT, fill=Y)
        return request_tab

    def create_response_tab(self): 
        def scroll_hdrs(*args):
            apply(self.res_lb1.yview, args)
            apply(self.res_lb2.yview, args)

        def select(e):
            row = self.res_lb1.nearest(e.y)
            for lb in ( self.res_lb1, self.res_lb2):
                lb.select_clear(0,END)
                lb.select_set(row)
            return 'break'
       
        response_tab=self._create_tab_blank("response", 2) 
        self.response_info=Label(response_tab, text='', justify=LEFT, \
                           font='Helvetica 8 bold', wraplength= self.w-20)
        self.response_info.pack(side=TOP, anchor=W) 
        headers_frame=Frame(response_tab)
        headers_frame.pack(expand=Y, fill=BOTH)
        self.res_lb1=Listbox(headers_frame, borderwidth=0, width=10, exportselection=FALSE)
        self.res_lb1.pack(side=LEFT, expand=Y, fill=BOTH) 
        self.res_lb2=Listbox(headers_frame, borderwidth=0, width=90, exportselection=FALSE)
        self.res_lb2.pack(side=LEFT, expand=Y, fill=BOTH)
        for lb in (self.res_lb1, self.res_lb2):
            lb.bind('<Button-1>', lambda e: select(e))
            lb.bind('<B1-Motion>', lambda e: select(e))          
            lb.bind('<Leave>', lambda e: 'break')      
        sb=Scrollbar(headers_frame, orient=VERTICAL, command=scroll_hdrs)
        self.req_lb1.config(yscrollcommand=sb.set)
        sb.pack(side=RIGHT, fill=Y)


    def create_tabs(self, parent):  
        self.tab_buttons_field=Frame(parent, relief=GROOVE, borderwidth=1)
        self.tab_buttons_field.pack(side=TOP, expand=N, fill=X)
       
        self.tab_body_field=Frame (parent)
        self.tab_body_field.pack (side=TOP, expand=Y, fill=BOTH)

        self.current_tab=self.create_summary_tab()
        self.create_request_tab()
        self.create_response_tab()
        self.current_tab.pack(expand=Y, fill=BOTH) #show the first tab          

    def display_tab(self, tab):
        self.current_tab.forget()
        tab.pack(expand=Y, fill=BOTH)
        self.current_tab=tab

    def add_record(self, rec):       
        self.records.append (rec)
        self.main_col_list[0].insert(END, rec.timestamp[:-7])
        self.main_col_list[1].insert(END, rec.text)
        self.show_last_record()

    def clear_tabs(self):
        self.summary_field.delete(1.0, END)
        self.response_info.config(text='')
        for lb in ( self.req_lb1 , self.req_lb2, self.res_lb1, self.res_lb2):
            lb.delete(0, END)

    def show_record(self, rec_no):
        rec=self.records[rec_no]
        self.clear_tabs()      
        rec.draw(self)

    def show_last_record(self):        
        if (self.showLastRecord.get()==1):
            self.main_col_list[0].see(END)
            self.main_col_list [1].see(END)

    def show_since_start(self):
        self.clear_tabs()
        self.main_col_list[0].delete(0, END)
        self.main_col_list [1].delete(0, END)
        self.ds.stop()
        self.ds=DataSupplier(self, loadLastOnly=self.showSinceStart.get())
        #self.ds.loadLastOnly=self.showSinceStart.get()
        self.ds.start()
            

    def feed_data(self):        
        while(True):
            try:
                rec=self.data_queue.get (block=False)
            except Queue.Empty:
                self.after(UI_UPDATE_INTERVAL, lambda: self.feed_data())
                break                
            else:                
                self.add_record(rec)

    def stop_data_supply(self):
        self.ds.stop()


class Record(object):
    def draw(self, widget):
        pass

class StartRecord(Record):
    def __init__(self, timestamp, text):
        self.timestamp=timestamp
        self.text=text
    def draw(self, widget):
        for i, button in enumerate(widget.tab_buttons ):
            if i==0:
                button.select()
                button.invoke()
            else: button.forget()              
        txt='TIMESTAMP: \t' + self.timestamp \
             + '\nTEXT: \t\t' + self.text
        widget.summary_field.insert(END, txt)


class RequestRecord(Record): 
    def __init__(self, id, process_id, thread_id, timestamp, text, headers):
        self.id=id
        self.process_id=process_id
        self.thread_id=thread_id
        self.timestamp=timestamp
        self.text=text
        self.headers=headers    

    def draw(self, widget):
        for i, button in enumerate(widget.tab_buttons):
            if i==0:
                pass
                #button.select() 
                #button.invoke()
            else: button.pack(side=LEFT)
        #show summary
        widget.summary_field.insert(END, "some summary text")
        # show request
        for k, v in self.headers.items():
            widget.req_lb1.insert(END, k)
            widget.req_lb2.insert(END, v)
        # get additional data
        crit='process_id=%i and thread_id=%i' % (self.process_id, self.thread_id)
        rows=search_log(criteria=crit, start_from=self.id, max_count=-1)
        # show data
        for rec in rows:
            if rec['type']=='HTTP:response': 
                t1=self.timestamp
                td1=timedelta(days=int(t1[8:10]), hours=int(t1[11:13]), minutes=int(t1[14:16]), \
                              seconds=int(t1[17:19]), microseconds=int(t1[20:])) 
                t2=rec['timestamp']
                td2=timedelta(days=int(t2[8:10]), hours=int(t2[11:13]), minutes=int(t2[14:16]), \
                              seconds=int(t2[17:19]), microseconds=int(t2[20:])) 
                delta=td2-td1
                delta=delta.seconds + 0.000001*delta.microseconds
                
                txt='TEXT: \t' + rec['text'] + \
                     '\nDELAY: \t' + str(delta) 
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
            if rec_type=='HTTP:GET':            
                record=RequestRecord(rec['id'], rec['process_id'], rec['thread_id'], rec['timestamp'], \
                                  rec_type+" "+rec['text'], rec['headers']) 
            elif rec_type=='HTTP:start':
                record=StartRecord(rec['timestamp'], rec['text']) 
            else:
                continue
            self.vw.data_queue.put(record)
        return len(rows)==self._max_count

    def stop(self):
        self.keepRunning=False

    def get_last_start_id(self): 
        _start_from=None
        while(True):
            rows=search_log(max_count=10, start_from=_start_from)
            if len(rows)==0: return 0
            for rec in rows:
                if rec['type']=='HTTP:start': 
                    return int(rec['id'])-1
                _start_from=rec['id']
        
   
    def run(self):        
        if (self.loadLastOnly==1):
            self.last_record_id=self.get_last_start_id ()
        else:
            self.last_record_id=1
        while self.load_data(self.last_record_id):
            pass # load all data
        while (self.keepRunning):
            self.load_data(self.last_record_id )
            time.sleep(DB_UPDATE_INTERVAL)



class WidgetRunner(threading.Thread):
    def run(self):
        root=Tk()
        vw=ViewerWidget(root)
        vw.feed_data()        
        root.mainloop()
        vw.stop_data_supply()

wr=WidgetRunner()
wr.start()

  
