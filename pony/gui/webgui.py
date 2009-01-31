import os.path

import pony

from pony.utils import read_text_file, markdown
from pony.web import http, url
from pony.webutils import link
from pony.templating import html, Html

@http('/pony/test')
def test():
    return html('''
    <h1>Content of request headers</h1>
    <table class="border">
      <tr><th>Header</th><th>Value</th></tr>
      $for(key, value in sorted(http.request.environ.items()))
      {
        <tr><td>$key</td><td>&nbsp;$value</td></tr>
      }
    </table>''')

@http('/pony/doc/$page?lang=$lang')
@http('/pony/doc/', redirect=True)
def docs(page='MainPage', lang=None):
    if lang:
        filename = os.path.join(pony.PONY_DIR, 'doc', '%s-%s.txt' % (page, lang))
        if not os.path.exists(filename): raise http.NotFound
    else:
        for lang in http.request.languages:
            filename = os.path.join(pony.PONY_DIR, 'doc', '%s-%s.txt' % (page, lang))
            if os.path.exists(filename): raise http.Redirect(url(docs, page, lang))
        else:
            filename = os.path.join(pony.PONY_DIR, 'doc', page + '.txt')
            if not os.path.exists(filename): raise http.NotFound

    text = read_text_file(filename)
    content = markdown(Html(text))
    return html('''
    $link('/pony/static/css/doc.css')
    <body><div class="container">
    <div class="span-20">$content</div>
    </div></body>
    ''')
