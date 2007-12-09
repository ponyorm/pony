from pony.main import *

use_autoreload()

@http('/?a=$a&b=$b')
def index(a=None, b=None):
    return html("""
    <form method="GET">
      <p><input type="text" name="a">
      <p><input type="text" name="b">
      <p><input type="submit" value="send">
    </form>
    <p>a = $a
    <p>b = $b
    """)

start_http_server()
