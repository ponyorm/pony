from pony.main import *

use_autoreload()

@http('/')
def index():
    a = get_param('a')
    b = get_param('b')
    return html("""
    <form method="post">
      <p><input type="text" name="a">
      <p><input type="text" name="b">
      <p><input type="submit" value="send">
    </form>
    <p>a = $a
    <p>b = $b
    """)

start_http_server()
