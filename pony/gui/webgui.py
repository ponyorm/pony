import pprint, threading, time
from datetime import timedelta

from pony import utils
from pony.web import http
from pony.logging import search_log

from pony.templating import html, template

@http('/pony/test')
def test():
    return html('''
    <h1>Content of request headers</h1>
    <table>
      <tr><th>Header</th><th>Value</th></tr>
      $for(key, value in sorted(http.request.environ.items()))
      {
        <tr><td>$key</td><td>&nbsp;$value</td></tr>
      }
    </table>''')
