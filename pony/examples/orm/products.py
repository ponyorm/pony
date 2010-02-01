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
    salary = Required(Decimal)
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
    employee = Required(Employee)
    customer = Required(Customer)
    date = Required(datetime)
    total = Required(decimal)
    items = Dict(Product, 'OrderItem')

class OrderItem(Entity):
    order = Required(Order)
    product = Required(Product)
    price = Required(decimal)
    quantity = Required(decimal)
    # amount = Required(decimal)
    PrimaryKey(order, product)


# 1. Магазины, расположенные в Московском районе

select(s for s in Shop if s.district.name == u'Московский')

# 2. Клиенты, покупавшие товары в Московском районе

select(c for c in Customer
         for s in Shop  if s.district.name == u'Московский'
         for o in Order if o.customer == c and o.shop == s)
         
select(c for c in Customer
         for s in Shop
         for o in Order if o.customer == c and o.shop == s and s.district.name == u'Московский')

select(o.customer for s in Shop if s.district.name == u'Московский'
                  for o in s.order)

select(c for c in Customer
         if exists(o for o in c.orders if o.shop.district.name == u'Московский'))

select(c for c in Customer
         if u'Московский' in select(o.shop.district.name for o in c.orders))

select(c for c in Customer
         if u'Московский' in select(o.shop.district.name for o in Orders
                                                         if o.customer == c))

select(c for c in Customer if u'Московский' in c.orders.shop.district.name)

select(s for s in Shop if s.district.name == u'Московский').orders.customer

# 3. Товары, находящиеся только в одном магазине

select(p for p in Product if len(p.shops) == 1)  # Не сработает если в Quantities допускается quantity==0

select(p for p in Product if 1 == len(select(q.shop for q in Quantities
                                                    if q.product == p and q.quantity > 0)))

select(p for p in Product if 1 == len(select(q for q in p.shops.values()
                                               if q.quantity > 0)))

# 4. Сотрудники, работающие в Московском районе
# 5. Товары, которых вообще нет на складе
# 6. Товары, которые есть во всех магазинах
# 7. Заказы, состоящие из нескольких товаров
# 8. Максимальная цена
# 9. Самый дорогой товар
# 10. Товар, принесший максимальную прибыль
