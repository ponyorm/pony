from pony.orm import *
from pony.db import Database

_diagram_ = Diagram()

class Customer(Entity):
    login = PrimaryKey(unicode)
    password = Required(unicode)
    name = Required(unicode)
    address = Required(unicode)
    cart_items = Set("CartItem")
    orders = Set("Order")
    reviews = Set("Review")

class Product(Entity):
    id = PrimaryKey(int)
    name = Required(unicode)
    desc = Optional(unicode)
    price = Required(unicode)
    count = Required(int)
    cart_items = Set("CartItem")
    order_items = Set("OrderItem")
    reviews = Set("Review")

class CartItem(Entity):
    customer = Required(Customer)
    product = Required(Product)
    count = Required(int)
    PrimaryKey(customer, product)

class Order(Entity):
    id = PrimaryKey(int)
    customer = Required(Customer)
    order_items = Set("OrderItem")
    price = Required(unicode)
    state = Required(unicode)
    date_created = Required(unicode)
    date_shipped = Optional(unicode)
    date_received = Optional(unicode)

class OrderItem(Entity):
    order = Required(Order)
    product = Required(Product)
    count = Required(int)
    price_per_item = Required(unicode)

class Review(Entity):
    customer = Required(Customer)
    product = Required(Product)
    text = Optional(unicode)
    rating = Required(int)
    date = Required(unicode)

db = Database('sqlite', ':memory:')
generate_mapping(db, check_tables = False)    