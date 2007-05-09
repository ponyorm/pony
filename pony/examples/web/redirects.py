from pony.main import *

use_autoreload()

@http('/')
def index():
    return html('''
    <h1>Examples of automatic redirection</h1>
    <ul>
      <li><a href="/my/old/link.php?param1=100&param2=200">This link</a>
          points to old page. If you click on link, you'll be
          automatically redirected to $link('this new page', example, 100, 200)
      <li><a href="/another/old/page.jsp?a=300&b=400">Another</a> old link;
          this is example of temporary redirects.
      <li>This is <a href="$url(example, 500, 600)">new page</a>;
      <li><a href="/my/new/link/700/800/">This is "almost correct" link</a>
          You can notice superfluous trailing slash, which is not present
          in correct link. If you follow the link, you'll be redirect
          on <a href="/my/new/link/700/800">correct page</a>.
      <li><a href="/example2">Another example of "trailing slash correction".
          In this case it'll be added
    </ul>
    ''')

@http('my/old/link.php?param1=$x&param2=$y', redirect=True)
@http('my/new/link/$x/$y')
@http('another/old/page.jsp?a=$x&b=$y', redirect='307 Temporary Redirect')
def example(x, y):
    return html('''
        <h1>My new page!</h1>
        <p>x = $x; y = $y
        ''')

@http('/example2/') # with trailing slash
def example2():
    return html('<h1>Hello again!</h1>')

start_http_server()
