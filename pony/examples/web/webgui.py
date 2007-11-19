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
            <td class="pad"><a href="details?id=$(r['id'])">$(r['id'])</a></td>
            <td class="pad"><a href="details?id=$(r['id'])">$(r['timestamp'][:-7])</a></td>
            <td class="pad"><a href="details?id=$(r['id'])">$(r['text'])</a></td>
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

def load_fragment(frm=None, to=None):
    print frm
    print to
    params = "type like 'HTTP:%' and type not in ('HTTP:response', 'HTTP:stop')"
    if frm is not None:
        data = search_log(-MAX_RECORD_DISPLAY_COUNT, frm, params)
    if to is not None:
        data = search_log(MAX_RECORD_DISPLAY_COUNT, to, params)
        data.reverse()
    return data

def get_details(rec_id):
    rec_id = int(rec_id)

    records = search_log(-1, None, "id = ?",[ rec_id ])
    req = records[0]
    rtype = req['type']
    if rtype == 'HTTP:start' or rtype == 'HTTP:stop':
        return ("No details",[],[],"","")

    req_headers = req['headers'].items()    
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
        status_text = ("%s; DELAY: %s; "
                "PROCESS_ID: %d; THREAD_ID: %d; RECORD_ID: %d" %
               (resp['text'], delta, process_id, thread_id, rec_id))        
        if req["user"] is not None: text += "; USER: %s" % req["user"]
        exceptions = search_log(-10, rec_id,
                                "type = 'exception' and id < ? "
                                "and process_id = ? and thread_id = ?",
                                [ resp['id'], process_id, thread_id ])
        if exceptions: status_text += "; EXCEPTION: " + exceptions[0]['text']

        resp_headers = resp['headers']

        exc_text_list = []
        for exc in exceptions:
            exc_text_list.append(exc['traceback'])
        exc_text = ('\n'.join(exc_text_list)).replace('\n', '<br/>')
        
        session_text = pprint.pformat(req['session'])
        return (status_text, req_headers, resp_headers, exc_text, session_text)
    else: return ("Error happened",[],[],"","")

@http('/details?id=$rec_id', type='text/html')
def show_details(rec_id=None):
    (status_text, req_headers, resp_headers, exc_text, session_text)=get_details(rec_id)
    return template("""
    <html><head>
    <style type="text/css">
      @import url("pony/static/css/webadmin.css");
    </style>
    </head><body>
    <h4>details for the record id=$rec_id</h4>
    
    <table>

      <tr class="delimeter"><td>status:</td></tr>
      <tr><td> $status_text </td></tr>

      <tr class="delimeter"><td>request:</td></tr> 
      <tr><td> 
        <table class="t2">
          <tr><th class="t2-header">HTTP request header</th><th>value</th></tr>
          $for(k, v in sorted(req_headers)) { <tr>
              <td>$k</td><td class="bordered">$v</td>
          </tr>
          }
        </table>
      </td></tr>

      <tr class="delimeter"><td>response:</td></tr>
      <tr><td>
      <table class="t2">
        <tr><th class="t2-header">HTTP response header</th><th>value</th></tr>
        $for(k, v in sorted(resp_headers)) { <tr>
            <td>$k</td><td>$v</td>
        </tr>
        }
      </table>
      </td></tr>

      <tr class="delimeter"><td>exceptions:</td></tr>  
      <tr><td class="del"> $exc_text
      </td></tr>

      <tr class="delimeter"><td>session</td></tr>  
      <tr><td> $session_text </td></tr>
    
    </table>
    </body></html>
        """)


@http('/details2?id=$rec_id', type='text/html')
def show_details2(rec_id=None):
    (status_text, req_headers, resp_headers, exc_text, session_text)=get_details(rec_id)
    return template("""
    <table>

      <tr class="delimeter"><td>status:</td></tr>
      <tr><td class="bordered"> $status_text </td></tr>

      <tr class="delimeter"><td>request:</td></tr> 
      <tr><td class="bordered"> 
        <table class="t2">
          <tr><th class="bordered">HTTP request header</th><th class="bordered">value</th></tr>
          $for(k, v in sorted(req_headers)) { <tr>
              <td class="bordered">$k</td><td class="bordered">$v</td>
          </tr>
          }
        </table>
      </td></tr>

      <tr class="delimeter"><td>response:</td></tr>
      <tr><td class="bordered">
      <table class="t2">
        <tr><th class="bordered">HTTP response header</th><th class="bordered">value</th></tr>
        $for(k, v in sorted(resp_headers)) { <tr>
            <td class="bordered">$k</td><td class="bordered">$v</td>
        </tr>
        }
      </table>
      </td></tr>

      <tr class="delimeter"><td>exceptions:</td></tr>  
      <tr><td class="bordered"> $exc_text
      </td></tr>

      <tr class="delimeter"><td>session</td></tr>  
      <tr><td class="bordered"> $session_text </td></tr>
    
    </table>
    
        """)



@http('/getUpdates?last_id=$last_id', type='text/html')
def get_updates(last_id=None):
    data = search_log(-1000, last_id, "type like 'HTTP:%' and type <> 'HTTP:response'")    
    return template("""
    {[
     $for(r in data) {
       {"col1":"$(r['id'])", "col2":"$(r['timestamp'][:-7])", "col3":"$(r['text'])"} }
       $separator{,}     
    ]}
    """)


@http('/getLastRecordsHtml2?frm=$frm&to=$to', type='text/html')
def feed2(frm=None, to=None):
    if frm or to:
        data = load_fragment(frm, to)
    else:
        data = load()
    records_count = len(data)            
    return template("""
    <html><head>
    <style type="text/css">
      @import url("pony/static/css/webadmin.css");
    </style>
     <script type="text/javascript" src="pony/static/js/webgui.js">
     </script> 

    <!-- Dependency -->
<script src="http://yui.yahooapis.com/2.3.1/build/yahoo/yahoo-min.js"></script>

<!-- Used for Custom Events and event listener bindings -->
<script src="http://yui.yahooapis.com/2.3.1/build/event/event-min.js"></script>

<!-- Source file -->
<script src="http://yui.yahooapis.com/2.3.1/build/connection/connection-min.js"></script>

   <script type="text/javascript">

 var oldValue = null;
 var oldId = null;
 var currId = null;
 var keepUpdating = true;
 var lastRecId = 0;
 var recordsCount;
 var MAX_ROW_COUNT = 30;

 function coll() {
    el = document.getElementById(oldId);
    el.innerHTML=oldValue;    
    oldId=null;
    currId=null;
 }

var AjaxObject = {
	handleSuccess:function(o){
	  if (oldId != null) {
	    el = document.getElementById(oldId);
	    el.innerHTML=oldValue;
	  }
	  oldId = currId;  
	  el = document.getElementById(currId);
	  oldValue = el.innerHTML;
	  document.getElementById(currId).innerHTML = "<td colspan='3' class='expanded'><div onclick='coll()'>"
	  + oldValue + "<br><div class='scrl'>"
	  + o.responseText;
	},        
	handleFailure:function(o){
           alert("failure " + o);
	},        
	startRequest:function(cid) {
	   YAHOO.util.Connect.asyncRequest('GET', '/details2?id=' + cid, callback, null);
	}

};
var callback = {
	success:AjaxObject.handleSuccess,
	failure:AjaxObject.handleFailure,
	scope: AjaxObject
};

 function disp(id) {  
  if (currId == id) return;
  currId=id;
  AjaxObject.startRequest(id);
 }
 
 function subscribe() {
  var divs=document.getElementsByTagName('div');
  for (var i=0; i<divs.length; i++) {
    if (divs[i].className == 'row') {
      f = "disp('" + divs[i].getAttribute('id') + "');";
      divs[i].onclick= new Function(f);
    }
  }
 }
 onload=function() {
   subscribe();
   update();
 }

 function addRow(col1, col2, col3) {
   var tbl=document.getElementById("main-table");
   var lastRow=tbl.rows.length;
   var row=tbl.insertRow(lastRow);
   var td=document.createElement("TD");
   td.setAttribute("class", "main-row"); 
   td.innerHTML="<div class='row' onclick='disp(" + col1 + ")' id='" 
                 + col1 + "'> <div class='col11'>"
                 + col1 + "</div> <div class='col12'>"
                 + col2 + "</div> <div class='col13'>"
                 + col3 + "</div> </div>" ;
   row.appendChild(td);                
   lastRecId=col1;
 }


var TableUpdate = {
	handleSuccess:function(o){
           rows=eval(o.responseText);
           for (var i=0; i< rows.length; i++) {
             addRow(rows[i].col1, rows[i].col2, rows[i].col3);
           }  

	},        
	handleFailure:function(o){
           alert("failure " + o);
	},        
	startRequest:function(cid) {
	   YAHOO.util.Connect.asyncRequest('GET', '/getUpdates?last_id=' + cid, callback2, null);
	}

};
var callback2 = {
	success:TableUpdate.handleSuccess,
	failure:TableUpdate.handleFailure,
	scope: TableUpdate
};

function update() {
  try {
    var tbl=document.getElementById("main-table");
    if (tbl.rows.length > MAX_ROW_COUNT) 
        return;
    lastRecId=tbl.rows[tbl.rows.length-1].cells[0].getElementsByTagName("DIV")[0].id;
    TableUpdate.startRequest(lastRecId);
  } finally {
    if (keepUpdating) 
        setTimeout( update, 1000);
  }
}


   </script>  


    </head><body>
    <h2>pony web admin</h2>
    <table id="main-table" border="1">
     <thead>
      <tr><td>
         <span class="col11">id</span> <span class="col12">timestamp</span> <span class="col13">type</span>
      </td></tr>     
     </thead>
     <tbody> 
        $for( r in data ) { 
        
        <tr><td class="main-row">
          <div class="row" id=$(r['id'])> 
            <span class="col11">$(r['id'])</span>
            <span class="col12">$(r['timestamp'][:-7])</span>
            <span class="col13">$(r['text'])</span>
          </div>
        </td></tr>
        }
      </tbody>
    </table>
    <div id="prevnext"></div>
    <input type="button" onclick="update()">
    </body></html>
    """)


if __name__ == '__main__':
    start_http_server()
    show_gui()
