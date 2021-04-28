from datetime               import datetime
from uuid                   import UUID, uuid4
from pony.orm               import *

db = Database()

class User(db.Entity):
    id = PrimaryKey(UUID, default=uuid4)
    created = Required(datetime, default=lambda: datetime.now())




db.bind('mssql', driver='ODBC Driver 17 for SQL Server', server='10.24.219.31', database='testing', username="tester2", password="tester123!")
db.generate_mapping(create_tables=True)

if __name__ == '__main__':
    with db_session:
        usr = User()
        users = select(u for u in User)[:]
        print(users)
