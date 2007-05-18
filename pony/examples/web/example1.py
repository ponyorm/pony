# -*- coding: cp1251 -*-

from pony.main import *

use_autoreload()

@http
def page1():
    return 'Hello, world!'

@http(type='text/html')
def page2():
    return """
              <h1>This is HTML page</h1>
              <p>click <a href="/">here</a> for return
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
    print '<p>click <a href="/">here</a> for return'
    print '</body>'
    print '</html>'

@printhtml
def header(title='Demonstration of Pony features'):
    print '<html>'
    print '<head>'
    print '<title>%s</title>' % title
    print '</head>'
    print '<body>'
    print '<h1>%s</h1>' % title

@printhtml
def footer():
    print '<p>click <a href="/">here</a> for return to main page'
    print '</body>'
    print '</html>'

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
    print footer()

start_http_server()
show_gui()
