from pony.main import *

use_autoreload()

@http('/')
def index():
    a = http['a']
    b = http['b']
    c = http['a', 'b']
    return html("""
    <form method="POST">
      <p><input type="text" name="a">
      <p><input type="text" name="b">
      <p><input type="submit" value="send">
    </form>
    <p>a = $a
    <p>b = $b
    <p>c = $c
    """)

http.start()
