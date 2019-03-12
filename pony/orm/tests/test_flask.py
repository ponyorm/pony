import unittest

from flask import Flask
from pony.flask import Pony
from pony.flask import _enter_session, _exit_session
from pony.orm import Database, Required, commit
from uuid import uuid4


def create_app():
    app = Flask(__name__)
    app.config.update(dict(
        DEBUG = False,
        ))

    Pony(app)
    
    db = Database('sqlite', ':memory:')
    
    class User(db.Entity):
        username = Required(str)

    db.generate_mapping(create_tables=True)

    @app.route("/hello_world")
    def hello_world():
        return "Hello World"

    @app.route("/create_user/<string:username>", methods=["POST"])
    def create(username):
        new_user = User(username=username)
        commit()
        return str(new_user.id)

    @app.route("/get_user/<int:id>")
    def get(id):
        return User[id].username

    return app


class TestFlask(unittest.TestCase):
    def test_hello_world(self):
        app = create_app()

        response = app.test_client().get("/hello_world")
        self.assertEqual(b"Hello World", response.data)

    def test_read_and_write(self):
        app = create_app()
        mock_username = uuid4().hex

        response = app.test_client().post("/create_user/{}".format(mock_username))
        self.assertEqual(200, response.status_code)
        created_id = int(response.data)

        response = app.test_client().get("/get_user/{}".format(created_id))
        self.assertEqual(200, response.status_code)
        self.assertEqual(mock_username, response.data.decode("UTF-8"))
