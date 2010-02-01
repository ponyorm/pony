from datetime import date, datetime
from decimal import Decimal

from pony.orm import *

class District(Entity):
    name = Unique(unicode)
    shops = Set('Shop')

class Shop(Entity):
    name = Unique(unicode)
    address = Unique(unicode)
    district = Required(District)
    employees = Set('Employee')
    products = Dict('Product', 'Quantity')
    orders = Set('Order')

class Person(Entity):
    first_name = Required(unicode)
    last_name = Required(unicode)

class Employee(Person):
    date_of_birth = Required(date)
    salary = Required(decimal)
    shop = Required(Shop)
    orders = Set(Order)

class Customer(Person):
    orders = Set(Order)

class ProductCategory(Entity):
    name = PrimaryKey(unicode)
    products = Set('Product')

class Product(Entity):
    name = Required(unicode)
    category = Required(ProductCategory)
    price = Required(Decimal)
    shops = Dict(Shop, 'Quantity')
    orders = Dict('Order', 'OrderItem')

class Quantity(Entity):
    shop = Required(Shop)
    product = Required(Product)
    quantity = Required(int)
    PrimaryKey(shop, product)

class Order(Entity):
    shop = Required(Shop)
    employee = Required('Employee')
    total = Required(decimal)
    items = Dict(Product, OrderItem)

class OrderItem(Entity):
    order = Required(Order)
    product = Required(Product)
    price = Required(decimal)
    quantity = Required(decimal)
    amount = Required(decimal)
    PrimaryKey(order, product)
