from __future__ import absolute_import, print_function

from decimal import Decimal
from datetime import datetime

from pony.converting import str2datetime
from pony.orm import *

db = Database("sqlite", "estore.sqlite", create_db=True)

class Customer(db.Entity):
    email = Required(str, unique=True)
    password = Required(str)
    name = Required(str)
    country = Required(str)
    address = Required(str)
    cart_items = Set("CartItem")
    orders = Set("Order")

class Product(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    categories = Set("Category")
    description = Optional(str)
    picture = Optional(buffer)
    price = Required(Decimal)
    quantity = Required(int)
    cart_items = Set("CartItem")
    order_items = Set("OrderItem")

class CartItem(db.Entity):
    quantity = Required(int)
    customer = Required(Customer)
    product = Required(Product)

class OrderItem(db.Entity):
    quantity = Required(int)
    price = Required(Decimal)
    order = Required("Order")
    product = Required(Product)
    PrimaryKey(order, product)

class Order(db.Entity):
    id = PrimaryKey(int, auto=True)
    state = Required(str)
    date_created = Required(datetime)
    date_shipped = Optional(datetime)
    date_delivered = Optional(datetime)
    total_price = Required(Decimal)
    customer = Required(Customer)
    items = Set(OrderItem)

class Category(db.Entity):
    name = Required(str, unique=True)
    products = Set(Product)

sql_debug(True)

db.generate_mapping(create_tables=True)

# Order states
CREATED = 'CREATED'
SHIPPED = 'SHIPPED'
DELIVERED = 'DELIVERED'
CANCELLED = 'CANCELLED'

@db_session
def populate_database():
    c1 = Customer(email='john@example.com', password='***',
                  name='John Smith', country='USA', address='address 1')

    c2 = Customer(email='matthew@example.com', password='***',
                  name='Matthew Reed', country='USA', address='address 2')

    c3 = Customer(email='chuanqin@example.com', password='***',
                  name='Chuan Qin', country='China', address='address 3')

    c4 = Customer(email='rebecca@example.com', password='***',
                  name='Rebecca Lawson', country='USA', address='address 4')

    c5 = Customer(email='oliver@example.com', password='***',
                  name='Oliver Blakey', country='UK', address='address 5')

    tablets = Category(name='Tablets')
    flash_drives = Category(name='USB Flash Drives')
    ssd = Category(name='Solid State Drives')
    storage = Category(name='Data Storage')

    p1 = Product(name='Kindle Fire HD', price=Decimal('284.00'), quantity=120,
                 description='Amazon tablet for web, movies, music, apps, '
                             'games, reading and more',
                 categories=[tablets])

    p2 = Product(name='Apple iPad with Retina Display MD513LL/A (16GB, Wi-Fi, White)',
                 price=Decimal('478.50'), quantity=180,
                 description='iPad with Retina display now features an A6X chip, '
                             'FaceTime HD camera, and faster Wi-Fi',
                 categories=[tablets])

    p3 = Product(name='SanDisk Cruzer 16 GB USB Flash Drive', price=Decimal('9.99'),
                 quantity=400, description='Take it all with you on reliable '
                                           'SanDisk USB flash drive',
                 categories=[flash_drives, storage])

    p4 = Product(name='Kingston Digital DataTraveler SE9 16GB USB 2.0',
                 price=Decimal('9.98'), quantity=350,
                 description='Convenient - small, capless and pocket-sized '
                             'for easy transportability',
                 categories=[flash_drives, storage])

    p5 = Product(name='Samsung 840 Series 2.5 inch 120GB SATA III SSD',
                 price=Decimal('98.95'), quantity=0,
                 description='Enables you to boot up your computer '
                             'in as little as 15 seconds',
                 categories=[ssd, storage])

    p6 = Product(name='Crucial m4 256GB 2.5-Inch SSD SATA 6Gb/s CT256M4SSD2',
                 price=Decimal('188.67'), quantity=60,
                 description='The award-winning SSD delivers '
                             'powerful performance gains for SATA 6Gb/s systems',
                 categories=[ssd, storage])

    CartItem(customer=c1, product=p1, quantity=1)
    CartItem(customer=c1, product=p2, quantity=1)
    CartItem(customer=c2, product=p5, quantity=2)

    o1 = Order(customer=c1, total_price=Decimal('292.00'), state=DELIVERED,
               date_created=str2datetime('2012-10-20 15:22:00'),
               date_shipped=str2datetime('2012-10-21 11:34:00'),
               date_delivered=str2datetime('2012-10-26 17:23:00'))

    OrderItem(order=o1, product=p1, price=Decimal('274.00'), quantity=1)
    OrderItem(order=o1, product=p4, price=Decimal('9.98'), quantity=2)

    o2 = Order(customer=c1, total_price=Decimal('478.50'), state=DELIVERED,
               date_created=str2datetime('2013-01-10 09:40:00'),
               date_shipped=str2datetime('2013-01-10 14:03:00'),
               date_delivered=str2datetime('2013-01-13 11:57:00'))

    OrderItem(order=o2, product=p2, price=Decimal('478.50'), quantity=1)

    o3 = Order(customer=c2, total_price=Decimal('680.50'), state=DELIVERED,
               date_created=str2datetime('2012-11-03 12:10:00'),
               date_shipped=str2datetime('2012-11-04 11:47:00'),
               date_delivered=str2datetime('2012-11-07 18:55:00'))

    OrderItem(order=o3, product=p2, price=Decimal('478.50'), quantity=1)
    OrderItem(order=o3, product=p4, price=Decimal('9.98'), quantity=2)
    OrderItem(order=o3, product=p6, price=Decimal('199.00'), quantity=1)

    o4 = Order(customer=c3, total_price=Decimal('99.80'), state=SHIPPED,
               date_created=str2datetime('2013-03-11 19:33:00'),
               date_shipped=str2datetime('2013-03-12 09:40:00'))

    OrderItem(order=o4, product=p4, price=Decimal('9.98'), quantity=10)

    o5 = Order(customer=c4, total_price=Decimal('722.00'), state=CREATED,
               date_created=str2datetime('2013-03-15 23:15:00'))

    OrderItem(order=o5, product=p1, price=Decimal('284.00'), quantity=1)
    OrderItem(order=o5, product=p2, price=Decimal('478.50'), quantity=1)

@db_session
def test_queries():

    print('All USA customers')
    print()
    result = select(c for c in Customer if c.country == 'USA')[:]

    print(result)
    print()

    print('The number of customers for each country')
    print()
    result = select((c.country, count(c)) for c in Customer)[:]

    print(result)
    print()

    print('Max product price')
    print()
    result = max(p.price for p in Product)

    print(result)
    print()

    print('Max SSD price')
    print()
    result = max(p.price for p in Product for cat in p.categories if cat.name == 'Solid State Drives')

    print(result)
    print()

    print('Three most expensive products:')
    print()
    result = select(p for p in Product).order_by(desc(Product.price))[:3]

    print(result)
    print()

    print('Out of stock products')
    print()
    result = select(p for p in Product if p.quantity == 0)[:]

    print(result)
    print()

    print('Most popular product')
    print()
    result = select(p for p in Product).order_by(lambda p: desc(sum(p.order_items.quantity))).first()

    print(result)
    print()

    print('Products that have never been ordered')
    print()
    result = select(p for p in Product if not p.order_items)[:]

    print(result)
    print()

    print('Customers who made several orders')
    print()
    result = select(c for c in Customer if count(c.orders) > 1)[:]

    print(result)
    print()

    print('Three most valuable customers')
    print()
    result = select(c for c in Customer).order_by(lambda c: desc(sum(c.orders.total_price)))[:3]

    print(result)
    print()

    print('Customers whose orders were shipped')
    print()
    result = select(c for c in Customer if SHIPPED in c.orders.state)[:]

    print(result)
    print()

    print('The same query with the INNER JOIN instead of IN')
    print()
    result = select(c for c in Customer if JOIN(SHIPPED in c.orders.state))[:]

    print(result)
    print()

    print('Customers with no orders')
    print()
    result = select(c for c in Customer if not c.orders)[:]

    print(result)
    print()

    print('The same query with the LEFT JOIN instead of NOT EXISTS')
    print()
    result = left_join(c for c in Customer for o in c.orders if o is None)[:]

    print(result)
    print()

    print('Customers which ordered several different tablets')
    print()
    result = select(c for c in Customer
                      for p in c.orders.items.product
                      if 'Tablets' in p.categories.name and count(p) > 1)[:]

    print(result)
    print()

    print('Customers which ordered several products from the same category')
    print()
    result = select((customer, category.name)
                    for customer in Customer
                    for product in customer.orders.items.product
                    for category in product.categories
                    if count(product) > 1)[:]

    print(result)
    print()

    print('Customers which ordered several products from the same category in the same order')
    print()
    result = select((customer, order, category.name)
                    for customer in Customer
                    for order in customer.orders
                    for product in order.items.product
                    for category in product.categories
                    if count(product) > 1)[:]

    print(result)
    print()

    print('Products whose price varies over time')
    print()
    result = select(p.name for p in Product if count(p.order_items.price) > 1)[:]

    print(result)
    print()

    print('The same query, but with min and max price for each product')
    print()
    result = select((p.name, min(p.order_items.price), max(p.order_items.price))
                    for p in Product if count(p.order_items.price) > 1)[:]

    print(result)
    print()

    print('Orders with a discount (order total price < sum of order item prices)')
    print()
    result = select(o for o in Order if o.total_price < sum(o.items.price * o.items.quantity))[:]

    print(result)
    print()


if __name__ == '__main__':
    with db_session:
        if Customer.select().first() is None:
            populate_database()
    test_queries()
