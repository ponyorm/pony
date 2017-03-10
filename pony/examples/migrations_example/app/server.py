from flask import Flask, request, jsonify

from pony import orm

from . import settings
from .models import db

app = Flask(__name__)


@app.route("/items")
def items():
    li = []
    descriptions = orm.select([item.url, item.description] for item in db.StoredItem)[:]
    for url, d in descriptions:
        li.append({
            'url': url,
            'description': d,
        })
    return jsonify(li)


def format(text):
    return {
        'id': text.id,
        'contents': text.contents,
    }


@app.route("/text/<id>")
def get_text(id):
    page = request.args.get('page', 5)
    text = db.Text[id]
    comments = orm.select(c for text in db.Text for c in text.comments if text.id == id)[:page]
    return jsonify({
        'text': text.contents,
        'comments': [format(c) for c in comments],
    })


@app.route("/texts")
def texts():
    texts = orm.select(t for t in db.Text)[:]
    return jsonify([format(t) for t in texts])


@app.route("/populate_texts")
def populate_texts():
    text1 = db.Text(contents='text1', url='/text1')
    text2 = db.Text(contents='text2', url='/text2')
    text3 = db.Text(contents='text3', url='/text3')
    text1.comments = [text2, text3]
    return jsonify({'success': True})


@app.route("/add")
def add():
    params = dict(request.args.items())
    typ = params.pop('type')
    entity = getattr(db, typ)
    entity(**params)
    return app.make_response('Added a %s: %s' % (typ, str(params)))


@app.before_request
def _():
    orm.db_session.__enter__()


@app.after_request
def _(response):
    orm.db_session.__exit__()
    return response


if __name__ == '__main__':
    db.connect(**settings.db_params)
    app.run()
