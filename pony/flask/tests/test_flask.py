import unittest

from flask import Flask
from pony.flask import Pony


def create_app():
    app = Flask(__name__)
    app.config.update(dict(
        DEBUG = False,
        SECRET_KEY = '0xDEADBEEF',
        PONY = {
            'provider': 'sqlite',
            'filename': 'db_flasktest.db3',
            'create_db': True
            }
        ))

    Pony(app)

    @app.route("/hello_world")
    def hello_world():
        return "Hello World"

    return app


class TestFlask(unittest.TestCase):
    def test_simple_read(self):
        app = create_app()

        response = app.test_client().get("/hello_world")
        self.assertEqual(b"Hello World", response.data)

