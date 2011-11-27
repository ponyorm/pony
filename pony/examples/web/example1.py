# -*- coding: cp1251 -*-

from pony.main import *

use_autoreload()

@http
def page1():
    "Returning plain text"
    return 'Hello, world!'

@http
def page2():
    name = '<World>'
    return html("<h1>Hello, @name!</h1>")

def header(title='Demonstration of Pony features'):
    return html('''
        <title>@title</title>
        <h1>@title</h1>
        <br>
    ''')

def footer():
    return html('<p>Click <a href="/">here</a> to return to main page')

@http('/mypage.html')
def page3():
    "Page with custom URL"
    return html('''
        @header('Demonstration of custom URL')
        <p>This page have custom URL
        <p>It also demonstrates how common parts 
        (such as header or footer) can be added to the page
        @footer()
    ''')

@http('/myblog/archives/$year/posts.html')
@http('/myblog/archives/$year/$month/posts.html')
@http('/myblog/$lang/archives/$year/posts.html')
@http('/myblog/$lang/archives/$year/$month/posts.html')
def page4(year, month=None, lang='en'):
    "Parameters encoded in URL"
    return html('''
        @header('My Blog Archives')
        <p>Demonstration how parameters can be encoded in URL
        <ul>
        <li>Language: <strong>@lang</strong>
        <li>Year: <strong>@year</strong>
        <li>Month: <strong>@(month or 'Not given')</strong>
        </ul>
        <p><a href="@url(page4, '2003')">Go to year 2003</a></p>
        <p><a href="@url(page4, 2005, 11, 'fr')">Go to French 2005-11</a></p>
        <p>@link('Go to English 2004-10', page4, 2004, 10)</p>
        @footer()
    ''')

@http('/hello?first_name=$name')
def page5(name=None):
    "Parameters in query part of URL"
    return html('''
        @header('URL parameters')
        @if(name is None)
        {
            <h2>What is your name?</h2>
            <form>
            <input type="text" name="first_name">
            <input type="submit" value="Send!">
            </form>
            <br>
            <p>You can try input something &quot;illegal&quot; instead of name, such as
            <p><code><strong>&lt;script&gt;alert(&quot;You are hacked!&quot;);&lt;/script&gt;</strong></code>
            <p>You will see as <strong>Pony</strong> automatically prevent such XSS (Cross-Site Scripting)
            attacks (those script will not be executed)</p>
        }
        @else
        {
            <h2>Hello, @name!</h2>
            <p><a href="@url(page5)">Try again</a></p>
        }
        @footer()
    ''')

@http
def page6():
    "Template defined in a separate file"
    return html()

@http
def page7():
    "Tabs"
    return html("""
    <link jquery plugins="tabs">
    
    <h1>Example of tabs</h1>
    
    @tabs()
    @+tab("One") {
            <h2>Nested tabs:</h2>
            @tabs(class_="span-12 prepend-1 append-1 last")
            @+tab("Nested tab 1"){<h2>Nested tab ONE</h2>}
            @+tab("Nested tab 2"){<h2>Nested tab TWO</h2>}
            @+tab("Tab with very very long name"){<h2>Nested tab THREE</h2>}
    }
    @+tab("Two") {
            <h2>Content of second tab</h2>
    }
    @+tab("Three") {
            <h2>Tab three</h2>
            <ul>
            <li>One
            <li>Two
            <li>Three
            </ul>
    }

    <br><br><br><hr>
    <p><a href="/">Return to main page</a>
    """)

@http
def page8():
    "This page deliberately contains error"
    a = u'проверка'
    b = None   # These lines will be visible in traceback
    c = a / b  # Point mouse onto variable name in browser to check its value
    return a

@http('/')
def index():
    "This is the root page"
    return html('''
        @header('Simple Pony examples')
        <ul>
        <li><h4><a href="@url(page1)">HelloWorld example</a></h4></li>
        <li><h4>@link('Simplest HTML page', page2)</h4></li>
        <li><h4>@link(page3)</h4></li>
        <li><h4>@link(page4, 2007, 10)</h4></li>
        <li><h4>@link(page5)</h4></li>
        <li><h4>@link(page6)</h4></li>
        <li><h4>@link(page7)</h4></li>
        <li><h4>@link(page8)</h4></li>
        </ul>
    ''')

if __name__ == '__main__':
    http.start()
    # show_gui()
