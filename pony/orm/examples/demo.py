from __future__ import absolute_import, print_function

from decimal import Decimal
from pony.orm import *

db = Database("sqlite", "demo.sqlite", create_db=True)

class Customer(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    email = Required(str, unique=True)
    orders = Set("Order")

class Order(db.Entity):
    id = PrimaryKey(int, auto=True)
    total_price = Required(Decimal)
    customer = Required(Customer)
    items = Set("OrderItem")

class Product(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    price = Required(Decimal)
    items = Set("OrderItem")

class OrderItem(db.Entity):
    quantity = Required(int, default=1)
    order = Required(Order)
    product = Required(Product)
    PrimaryKey(order, product)

sql_debug(True)
db.generate_mapping(create_tables=True)

def populate_database():
    c1 = Customer(name='John Smith', email='john@example.com')
    c2 = Customer(name='Matthew Reed', email='matthew@example.com')
    c3 = Customer(name='Chuan Qin', email='chuanqin@example.com')
    c4 = Customer(name='Rebecca Lawson', email='rebecca@example.com')
    c5 = Customer(name='Oliver Blakey', email='oliver@example.com')

    p1 = Product(name='Kindle Fire HD', price=Decimal('284.00'))
    p2 = Product(name='Apple iPad with Retina Display', price=Decimal('478.50'))
    p3 = Product(name='SanDisk Cruzer 16 GB USB Flash Drive', price=Decimal('9.99'))
    p4 = Product(name='Kingston DataTraveler 16GB USB 2.0', price=Decimal('9.98'))
    p5 = Product(name='Samsung 840 Series 120GB SATA III SSD', price=Decimal('98.95'))
    p6 = Product(name='Crucial m4 256GB SSD SATA 6Gb/s', price=Decimal('188.67'))

    o1 = Order(customer=c1, total_price=Decimal('292.00'))
    OrderItem(order=o1, product=p1)
    OrderItem(order=o1, product=p4, quantity=2)

    o2 = Order(customer=c1, total_price=Decimal('478.50'))
    OrderItem(order=o2, product=p2)

    o3 = Order(customer=c2, total_price=Decimal('680.50'))
    OrderItem(order=o3, product=p2)
    OrderItem(order=o3, product=p4, quantity=2)
    OrderItem(order=o3, product=p6)

    o4 = Order(customer=c3, total_price=Decimal('99.80'))
    OrderItem(order=o4, product=p4, quantity=10)

    o5 = Order(customer=c4, total_price=Decimal('722.00'))
    OrderItem(order=o5, product=p1)
    OrderItem(order=o5, product=p2)

    commit()

