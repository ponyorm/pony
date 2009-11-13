from pony.main import *
import os

use_autoreload()

@http("/")
def index():
    IMAGES_PATH = "static/images/demo/"
    imagelist = [ IMAGES_PATH + s for s in os.listdir(IMAGES_PATH) if s.endswith(".jpg")]
    return html()

	
http.start()