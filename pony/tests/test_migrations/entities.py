from pony import orm

db = orm.Database('mysql', db='testdb', host='localhost', passwd='ponytest', user='ponytest')


class Activity(db.Entity):
    descr = orm.Required(str)