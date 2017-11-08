from datetime import datetime
from pony.orm import *


db = Database()


class User(db.Entity):
    username = Required(unicode, unique=True)
    password = Required(unicode)
    email = Required(unicode, unique=True)
    dt = Required(datetime, default=datetime.now)
    following = Set('Following', reverse='follower')
    followers = Set('Following', reverse='followee')
    photos = Set('Photo')
    likes = Set('Like')
    comments = Set('Comment', reverse='user')
    mentioned = Set('Comment', reverse='mentioned')


class Photo(db.Entity):
    picture = Required(buffer)
    dt = Required(datetime, default=datetime.now)
    tags = Set('Tag')
    user = Required(User)
    liked = Set('Like')
    comments = Set('Comment')


class Tag(db.Entity):
    name = PrimaryKey(unicode)
    photos = Set(Photo)


class Comment(db.Entity):
    photo = Required(Photo)
    user = Required(User, reverse='comments')
    dt = Required(datetime, default=datetime.now)
    text = Required(unicode)
    mentioned = Set(User, reverse='mentioned')


class Like(db.Entity):
    user = Required(User)
    photo = Required(Photo)
    dt = Required(datetime, default=datetime.now)
    PrimaryKey(user, photo)


class Following(db.Entity):
    follower = Required(User, reverse='following')
    followee = Required(User, reverse='followers')
    dt = Required(datetime, default=datetime.now)
    PrimaryKey(follower, followee)


db.connect("sqlite", ":memory:", create_db=True, create_tables=True)