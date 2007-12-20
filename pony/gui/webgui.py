from pony.main import *
from pony import utils
from pony.logging import search_log

import pprint, threading, time
from datetime import timedelta
from pony.templating import template

@http('/pony/test')
@printhtml
def test():
    print '<h1>Content of request headers</h1>'
    print '<table border="1">'
    print '<tr><th>Header</th><th>Value</th></tr>'
    for key, value in sorted(get_request().environ.items()):
        if value == '': value='&nbsp;'
        print '<tr><td>%s</td><td>%s</td></tr>' % (key, value)
    print '</table>'


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
    <html><head>
    <style type="text/css">
      @import url("pony/static/css/webadmin.css");
    </style>
    </head><body>
    <h2>pony web admin</h2>
    <table border="0">
        <tr><th>id</th><th>timestamp</th><th>type</th></tr>
        $for(r in data) { 
        
        <tr class="row">
            <td><a href="details?id=$(r['id'])">$(r['id'])</a></td>
            <td><a href="details?id=$(r['id'])">$(r['timestamp'][:-7])</a></td>
            <td><a href="details?id=$(r['id'])">$(r['text'])</a></td>
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


@http('/details?id=$rec_id', type='text/html')
def get_record(rec_id=None):
    rec_id = int(rec_id)

    records = search_log(-1, None, "id = ?",[ rec_id ])
    req = records[0]
    rtype = req['type']
    if rtype == 'HTTP:start' or rtype == 'HTTP:stop':
        return "No details"

    req_headers = req['headers']    
    process_id = req['process_id']
    thread_id = req['thread_id']

    records = search_log(-1, rec_id,
        "type like 'HTTP:%' and process_id = ? and thread_id = ?",
        [ process_id, thread_id ])

    if records and records[0]['type'] == 'HTTP:response':
        resp = records[0]

        rec_id = req['id']
        
        dt1 = utils.timestamp2datetime(req['timestamp'])
        dt2 = utils.timestamp2datetime(resp['timestamp'])
        delta = dt2 - dt1
        delta=delta.seconds + 0.000001 * delta.microseconds
        text = ("%s; DELAY: %s; "
                "PROCESS_ID: %d; THREAD_ID: %d; RECORD_ID: %d" %
               (resp['text'], delta, process_id, thread_id, rec_id))        
        if req["user"] is not None: text += "; USER: %s" % req["user"]
        exceptions = search_log(-10, rec_id,
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
    <html><head>
    <style type="text/css">
      @import url("pony/static/css/webadmin.css");
    </style>
    </head><body>

<table>
   <tr><td>
    <div align="center"> 
    <h2>details for the record id=$rec_id</h2>    
    </div>
    <div align="right">
      <a href="/getLastRecordsHtml">back<a>
    </dev>
   </td></tr> 
   <tr class="delimeter"><td>status:</td></tr>

    <tr><td> 
    $text
    </td></tr>

    <tr class="delimeter"><td>request:</td></tr> 

    <tr><td class="details-container"> 
    <table class="t2">
        <tr><th class="t2-header">HTTP request header</th><th>value</th></tr>
        $for(k, v in sorted(req_headers.items())) { <tr>
            <td>$k</td><td width="50%">$v</td>
        </tr>
        }
    </table>
    </td></tr>

    <tr class="delimeter"><td>response:</td></tr> 

    <tr><td class="details-container">
    <table class="t2">
        <tr><th class="t2-header">HTTP response header</th><th>value</th></tr>
        $for(k, v in sorted(resp_headers)) { <tr>
            <td>$k</td><td>$v</td>
        </tr>
        }
    </table>
    </td></tr>

    <tr class="delimeter"><td>exceptions:</td></tr>  
    <tr><td class="del">
    $exc_text
    </td></tr>

    <tr class="delimeter"><td>session</td></tr>  
    <tr><td class="del">
     $session_text
</td></tr></table>
    </body></html>


        """)

    else: return "Error happened"        

