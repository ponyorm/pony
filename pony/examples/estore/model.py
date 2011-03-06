from datetime import datetime
from decimal import Decimal

from pony.orm import *
from pony.db import Database

_diagram_ = Diagram()

class Product(Entity):
    id = PrimaryKey(int)
    name = Required(unicode)
    desc = Optional(unicode)
    price = Required(Decimal, 10, 2)
    warehouse_quantity = Required(int)
    customer_carts = Set("Customer")
    order_items = Set("OrderItem")
    reviews = Set("Review")

class Customer(Entity):
    id = PrimaryKey(int)
    login = Unique(unicode)
    password = Required(unicode)
    name = Required(unicode)
    address = Required(unicode)
    cart = Set("Product")
    orders = Set("Order")
    reviews = Set("Review")

class Order(Entity):
    customer = Required(Customer)
    items = Set("OrderItem")
    total_price = Required(Decimal, 10, 2)
    state = Required(unicode)
    date_created = Required(datetime)
    date_shipped = Optional(datetime)
    date_received = Optional(datetime)

class OrderItem(Entity):
    order = Required(Order)
    product = Required(Product)
    quantity = Required(int)
    item_price = Required(Decimal, 10, 2)
    PrimaryKey(order, product)

class Review(Entity):
    customer = Required(Customer)
    product = Required(Product)
    rating = Required(int)
    text = Optional(unicode)
    date = Required(datetime)
    Unique(customer, product)

db = Database('sqlite', ':memory:')
generate_mapping(db, check_tables = False)    