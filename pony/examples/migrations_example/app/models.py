from pony.orm import *

db = Database()


class StoredItem(db.Entity):
    url = Required(str)
    description = Optional(str)


class Video(StoredItem):
    duration_sec = Optional(int)


class Text(StoredItem):
    contents = Required(str)
    comments = Set(lambda: Text)
    commented = Set(lambda: Text)


class Picture(StoredItem):
    width = Optional(int)
    height = Optional(int)
    f = Optional(str)
