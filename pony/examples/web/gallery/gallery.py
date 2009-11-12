from pony.main import * 

use_autoreload()

@http("/")
def index():
    return html()

	
#show_gui()
http.start()