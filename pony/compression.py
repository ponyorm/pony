from __future__ import absolute_import, print_function

from cStringIO import StringIO
from cgi import parse_header
from gzip import GzipFile
from zlib import compress

import pony
from pony.utils import decorator

compressable_mime_types = set("""
    application/javascript
    application/json
    application/msword
    application/pdf
    application/postscript
    application/rtf
    application/x-javascript
    application/xml
    """.split())

min_compressed_length = 500

@decorator
def compression_middleware(app, environ):
    status, headers, content = app(environ)

    accept_encoding = environ.get('HTTP_ACCEPT_ENCODING')
    # Should we do proper parsing of pervert Accept-Encoding headers such as "gzip;q=0, *" ?
    if not accept_encoding: return status, headers, content

    hdict = dict(headers)
    if 'Content-Encoding' in hdict: return status, headers, content

    mime_type = hdict.get('Content-Type', 'text/plain').split(';', 1)[0]
    if mime_type.startswith('text/'): pass
    elif mime_type.endswith('+xml'): pass
    elif mime_type in compressable_mime_types: pass
    else: return status, headers, content

    if hasattr(content, 'read'): content = content.read()  # read string from file-like object
    else: assert isinstance(content, str)
    if len(content) < min_compressed_length: return status, headers, content
    if 'gzip' in accept_encoding:
        io = StringIO()
        zfile = GzipFile(fileobj=io, mode='wb')
        zfile.write(content)
        zfile.close()
        compressed = io.getvalue()
        encoding = 'gzip'
    elif 'deflate' in accept_encoding:
        compressed = compress(content)[2:-4]
        encoding = 'deflate'
    else: return status, headers, content

    if len(compressed) > len(content): return status, headers, content
    headers = [ header for header in headers if header[0] != 'Content-Length' ]
    headers.append(('Content-Length', str(len(compressed))))
    headers.append(('Content-Encoding', encoding))
    return status, headers, compressed

if pony.MODE.startswith('GAE-'):

    # Google currently does not allow you to set Content-Encoding headers,
    # but it appllyes gzip encoding automatically for html and css files
    def compression_middleware(app):
        return app
