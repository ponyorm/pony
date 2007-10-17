# -*- coding: cp1251 -*-

from pony.main import *
from pony import utils
from pony.logging import search_log

import pprint, threading, time
from datetime import timedelta

use_autoreload()

@http('/getLastRecords')
def feed():
    data = load()
    records_count = len(data)
    return template("""
    {"recordsReturned":$records_count,"records":[
        $for(r in data) {          
            {"id":"$(r['id'])", "ts":"$(r['timestamp'][:-7])", "type": "$(r['text'])"} }
        $separator{,}
    ]}
    """)

@http('/getLastRecordsHtml', type='text/html')
def feed2():
    data = load()
    records_count = len(data)
    return template("""
    <html><head></head><body><table border="1">
        <tr><th>id</th><th>timestamp</th><th>type</th><th></th></tr>
        $for(r in data) { <tr>
            <td>$(r['id'])</td><td>$(r['timestamp'][:-7])</td><td>$(r['text'])</td>
               <td><a href="more?record_id=$(r['id'])">+</a></td>
        </tr>
        }
    </table></body></html>
    """)


def load(since_last_start=True):
    start_id = 0
    if since_last_start:
        start = search_log(1, None, "type='HTTP:start'")
        if start: start_id = start[0]['id']
    data = search_log(MAX_RECORD_DISPLAY_COUNT, None,
        "type like 'HTTP:%' and type not in ('HTTP:response', 'HTTP:stop') and id >= ?",
        [ start_id ])
    data.reverse()
    return data

MAX_RECORD_DISPLAY_COUNT = 1000

from pony.templating import template

def load2(since_last_start=True):
    start_id = 0
    if since_last_start:
        start = search_log(1, None, "type='HTTP:start'")
        if start: start_id = start[0]['id']
    data = search_log(MAX_RECORD_DISPLAY_COUNT, None,
        "type like 'HTTP:%' and type <> 'HTTP:response' and id >= ?",
        [ start_id ])
    data.reverse()
    json = '{"recordsReturned":1397,"totalRecords":1397,"startIndex":0,"sort":null,"dir":"asc","records":['
    comma = ""
    for r in data:
        rtype = r['type']
        if rtype == 'HTTP:start': record = '{"id":"%s", "ts":"%s", "type":"%s"}' % (r['id'], r['timestamp'][:-7], r['text'])  

        #text= self.data['text']
        #process_id = self.data['process_id']
        #thread_id = self.data['thread_id']
        #record_id = self.data['id']


        elif rtype == 'HTTP:stop': pass
        else: record = '{"id":"%s", "ts":"%s", "type":"%s"}' % (r['id'], r['timestamp'][:-7], r['text'])
        json = json + comma + record
        comma = ","
    json = json + "]}"
    return json

@http('/webintf/request')
@printhtml
def session_feed():
    print """
<table id="requestTable" border="1" width="100%">
<thead>
<tr width="50%">
  <td>HTTP request header</td>
  <td>value</td>
</tr>
</thead>
<tbody>
<tr>
  <td>ACTUAL_SERVER_PROTOCOL</td>
  <td>HTTP/1.1</td>
</tr>
</tbody>
</table>
    """
@http('/more?record_id=$record_id')
def get_record(record_id=None):
    record_id = int(record_id)

    records = search_log(-1, None, "id = ?",[ record_id ])
    req = records[0]

    for k, v in sorted(req['headers'].items()):
        #widget.request_headers.add(k, v)
        print "headers= %s %s" % (k, v)
    
    process_id = req['process_id']
    thread_id = req['thread_id']

    records = search_log(-1, record_id,
        "type like 'HTTP:%' and process_id = ? and thread_id = ?",
        [ process_id, thread_id ])

    if records and records[0]['type'] == 'HTTP:response':
        resp = records[0]

        record_id = req['id']
        
        dt1 = utils.timestamp2datetime(req['timestamp'])
        dt2 = utils.timestamp2datetime(resp['timestamp'])
        delta = dt2 - dt1
        delta=delta.seconds + 0.000001 * delta.microseconds
        text = ("STATUS: %s; DELAY: %s; "
                "PROCESS_ID: %d; THREAD_ID: %d; RECORD_ID: %d" %
               (resp['text'], delta, process_id, thread_id, record_id))        
        if req["user"] is not None: text += "; USER: %s" % req["user"]
        exceptions = search_log(-10, record_id,
                                "type = 'exception' and id < ? "
                                "and process_id = ? and thread_id = ?",
                                [ resp['id'], process_id, thread_id ])
        if exceptions: text += "; EXCEPTION: " + exceptions[0]['text']
        #widget.response_info.config(text=text)
        print "resp=" + text
        for k, v in sorted(resp['headers']):
            #widget.response_headers.add(k, v)
            print "headers= %s %s" % (k, v)
        exc_text = []
        for exc in exceptions:
            exc_text.append(exc['traceback'])
        #widget.exceptions_field.insert(END, '\n'.join(exc_text))
        print "exc=" + '\n'.join(exc_text)
        
        session_text = pprint.pformat(req['session'])
        #widget.session_field.insert(END, session_text)
        print "sess=" + session_text

    else: Record.draw(self)


if __name__ == '__main__':
    #get_record()
    start_http_server()
    show_gui()
