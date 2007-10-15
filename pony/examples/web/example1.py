# -*- coding: cp1251 -*-

from pony.main import *
from pony import utils
from pony.logging import search_log

use_autoreload()

@http
def page1():
    return 'Hello, world!'

@http(type='text/html')
def page2():
    return """
              <h1>This is HTML page</h1>
              <p>Click <a href="/">here</a> for return
           """

@http
def page3():
    "This page deliberately contains error"
    a = 'hello'
    b = 10
    c = 1/0
    return a
    
@http
@printhtml
def page4():
    "Using of <code><strong>@printhtml</strong></code> decorator"
    print '<html>'
    print '<head><title>Page 3</title></head>'
    print '<body>'
    print '<h1>Hello again!</h1>'
    print '<p>Demonstration of'
    print '<code><strong>@printhtml</strong></code> decorator'
    print '<p>Click <a href="/">here</a> for return'
    print '</body>'
    print '</html>'

@printhtml
def header(title='Demonstration of Pony features'):
    print '<title>%s</title>' % title
    print '<h1>%s</h1>' % title

@printhtml
def footer():
    print '<p>Click <a href="/">here</a> to return to main page'

@http('mypage.html')
@printhtml
def page5():
    "Page with custom URL"
    print header('Demonstration of custom URL')
    print '<p>This page have custom URL'
    print '<p>It also demonstrate how common parts of page'
    print '(such as header or footer) can be factored out'
    print footer()

@http('/myblog/archives/$year/posts.html')
@http('/myblog/archives/$year/$month/posts.html')
@http('/myblog/$lang/archives/$year/posts.html')
@http('/myblog/$lang/archives/$year/$month/posts.html')
@printhtml
def page6(year, month=None, lang='en'):
    "Parameters encoded in URL"
    print header('My Blog Archives')
    print '<p>Demonstration how parameters can be encoded in URL'
    print '<ul>'
    print '<li>Language: <strong>%s</strong>' % lang
    print '<li>Year: <strong>%s</strong>' % year
    print '<li>Month: <strong>%s</strong>' % (month or 'Not given')
    print '</ul>'
    print '<p><a href="%s">Go to year 2003</a></p>' % url(page6, '2003')
    print ('<p><a href="%s">Go to French 2005-11</a></p>'
           % url(page6, 2005, 11, 'fr'))
    print '<p>%s</p>' % link('Go to English 2004-10', page6, 2004, 10)
    print footer()

@http('/hello?first_name=$name')
@printhtml
def page7(name=None):
    "Parameters in query part of URL"
    print header('URL parameters')
    if name is None:
        print '<h2>What is your name?</h2>'
        print '<form>'
        print '<input type="text" name="first_name">'
        print '<input type="submit" value="Send!">'
        print '</form>'
        print '<br>'
        print '<p>You can try input something &quot;illegal&quot;'
        print 'instead of name, such as '
        print '<code><strong>&lt;script&gt;'
        print 'alert(&quot;You are hacked!&quot;);'
        print '&lt;/script&gt;</strong></code>'
        print '<p>You will see as <strong>Pony</strong> automatically prevent'
        print 'such XSS (Cross-Site Scripting) attacks '
        print '(those script will not be executed)</p>'
    else:
        print '<h2>Hello, %s!</h2>' % name
        print '<p><a href="%s">Try again</a></p>' % url(page7)
    print footer()

@http('/') # This is root page
@printhtml
def index():
    print header('Simple Pony examples')
    print '<ul>'
    print '<li><a href="%s">HelloWorld example</a></li>' % url(page1)
    print '<li>%s</li>' % link('Simplest HTML page', page2)
    print '<li>%s</li>' % link(page3)
    print '<li>%s</li>' % link(page4)
    print '<li>%s</li>' % link(page5)
    print '<li>%s</li>' % link(page6, 2007, 10)
    print '<li>%s</li>' % link(page7)
    print '</ul>'

@http('/feed')
@printhtml
def feed():
#    print r"""
#{"recordsReturned":1397,"totalRecords":1397,"startIndex":0,"sort":null,"dir":"asc","records":[{"ts":"2007-10-11 00:34:02","type":"Start"}, {"ts":"2007-10-11 00:34:03","type":"Start2"}]}
#    """
    print load()

MAX_RECORD_DISPLAY_COUNT = 1000

def load(since_last_start=True):
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
        if rtype == 'HTTP:start': record = '{"ts":"%s", "type":"%s"}' % (r['timestamp'][:-7], r['text'])  

        #text= self.data['text']
        #process_id = self.data['process_id']
        #thread_id = self.data['thread_id']
        #record_id = self.data['id']


        elif rtype == 'HTTP:stop': pass
        else: record = '{"ts":"%s", "type":"%s"}' % (r['timestamp'][:-7], r['text'])
        json = json + comma + record
        comma = ","
    json = json + "]}"
    return json


if __name__ == '__main__':
    start_http_server()
    show_gui()
