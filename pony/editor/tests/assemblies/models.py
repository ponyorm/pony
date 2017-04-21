from pony.orm import *


db = Database()


class Assembly(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(unicode)
    assembly_items = Set('AssemblyItem')
    subassemblies = Set('Assembly', reverse='parent_assemblies')
    parent_assemblies = Set('Assembly', reverse='subassemblies')


class Component(db.Entity):
    id = PrimaryKey(int, auto=True)
    assembly_items = Set('AssemblyItem')
    part_number = Required(unicode, unique=True)
    stock_items = Set('StockItem')


class Resistor(Component):
    pass


class Capacitor(Component):
    pass


class AssemblyItem(db.Entity):
    assembly = Required(Assembly)
    component = Required(Component)
    quantity = Required(int)
    PrimaryKey(assembly, component)


class StockRoom(db.Entity):
    id = PrimaryKey(int, auto=True)
    location = Required(unicode)
    stock_items = Set('StockItem')


class Inductor(Component):
    pass


class Diode(Component):
    pass


class Transistor(Component):
    pass


class StockItem(db.Entity):
    stockroom = Required(StockRoom)
    component = Required(Component)
    quantity = Required(int)
    PrimaryKey(stockroom, component)


db.connect("sqlite", ":memory:", create_db=True, create_tables=True)