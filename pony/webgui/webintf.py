# -*- coding: cp1251 -*-

from pony.main import *
from pony import utils
from pony.logging import search_log

import pprint, threading, time
from datetime import timedelta
from pony.templating import template

use_autoreload()

MAX_RECORD_DISPLAY_COUNT = 1000

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
    <html><head></head><body>
    <table border="1">
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


@http('/more?record_id=$record_id', type='text/html')
def get_record(record_id=None):
    record_id = int(record_id)

    records = search_log(-1, None, "id = ?",[ record_id ])
    req = records[0]
    rtype = req['type']
    if rtype == 'HTTP:start' or rtype == 'HTTP:stop':
        return "No more"

    req_headers = req['headers']    
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

        resp_headers = resp['headers']

        exc_text_list = []
        for exc in exceptions:
            exc_text_list.append(exc['traceback'])
        exc_text = ('\n'.join(exc_text_list)).replace('\n', '<br/>')
        
        session_text = pprint.pformat(req['session'])

        return template("""
    <html><head></head><body>
    <div>
    $text
    </div> 
    <table border="1">
        <tr><th>HTTP request header</th><th>value</th></tr>
        $for(k, v in sorted(req_headers.items())) { <tr>
            <td>$k</td><td>$v</td>
        </tr>
        }
    </table>
    <br />
    <table border="1">
        <tr><th>HTTP response header</th><th>value</th></tr>
        $for(k, v in sorted(resp_headers)) { <tr>
            <td>$k</td><td>$v</td>
        </tr>
        }
    </table>
    <div>
      exceptions:<br/> $exc_text
    </div>
    <br />
    <div>
      session:<br/> $session_text
    </div>
    </body></html>


        """)

    else: return "Error happened"


if __name__ == '__main__':
    start_http_server()
    show_gui()
