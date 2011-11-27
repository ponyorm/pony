# coding: cp1251

from pony.main import *

@component(css="mycss.css", js=["myscipt1.js", "myscript2.js"])
def my_component(content):
    return html('<div class="my_class">@content</div>')

@http('/')
def index():
    return html(u'''
    <title>Пример использования компонента</title>
    <h1>Это заголовок</h1>
    @my_component("Hello")
    @my_component{Пример использования компонента}
    <p>Это просто текст</p>
    ''')

http.start()
