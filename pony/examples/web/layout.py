# -*- coding: cp1251 -*-

from pony.main import *

use_autoreload()

def header(title):
    return html('''
        <title>$title</title>
        <header>
          <h1>$title</h1>
        </header>
        ''')

@http("/")
def index():
    return html()

@http
def page1():
    pass


def sidebar():
    return html("")
    

start_http_server()