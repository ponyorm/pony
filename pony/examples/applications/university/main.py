from pony.main import *

http_server.start('university', host='localhost', port=8080,
                  db=Database('sqlite', 'c:\\university.sqlite'))
