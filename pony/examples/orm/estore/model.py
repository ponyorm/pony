from datetime import date
from decimal import Decimal

from pony.orm import *
from pony.db import Database

_diagram_ = Diagram()

class Customer(Entity):
    login = PrimaryKey(unicode)
    password = Required(unicode)
    name = Required(unicode)
    address = Required(unicode)
    products = Set("Product")
    orders = Set("Order")

class Product(Entity):
    id = PrimaryKey(int)
    name = Required(unicode)
    description = Required(unicode)
    picture = Optional(buffer)
    price = Required(Decimal, 7, 2)
    category = Required(unicode)
    quantity = Required(int)
    customers = Set("Customer")
    order_items = Set("OrderItem")

class Order(Entity):
    id = PrimaryKey(int, auto=True)
    date_created = Required(date, default=date.today)
    date_delivered = Optional(date)
    date_received = Optional(date)
    state = Required(unicode, default="CREATED")
    total_price = Required(Decimal, 7, 2, default=Decimal("0.0"))
    customer = Required("Customer")
    order_items = Set("OrderItem")

class OrderItem(Entity):
    item_price = Required(Decimal, 7, 2)
    quantity = Required(int)
    order = Required("Order")
    product = Required("Product")
    PrimaryKey(order, product)

db = Database('sqlite', ':memory:')
sql_debug(False)
generate_mapping(db, create_tables=True)
sql_debug(True)