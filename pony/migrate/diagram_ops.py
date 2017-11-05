from __future__ import absolute_import, print_function, division
from pony.py23compat import basestring

from inspect import isfunction
from copy import deepcopy

from .utils import deconstructible
from .serializer import serialize


@deconstructible
class Operation(object):

    def __init__(op, **kwargs):
        forward = kwargs.get('forward')
        if forward:
            op.forward = forward

    @property
    def is_custom(self):
        forward = getattr(self, 'forward', None)
        return bool(forward)

    def __repr__(op):
        return serialize(op)

    def apply(op, db):
        assert False, 'abstract method'

class AddEntity(Operation):
    def __init__(op, entity_name, base_names, attrs, **kw):
        Operation.__init__(op, **kw)
        op.entity_name = entity_name
        op.base_names = base_names
        op.attrs = attrs
    def apply(op, db):
        attrs = [
            (name, _clone_attr(attr)) for name, attr in op.attrs
        ]
        db._add_entity_(op.entity_name, op.base_names, attrs)

class RemoveEntity(Operation):
    def __init__(op, entity_name, **kw):
        Operation.__init__(op, **kw)
        op.entity_name = entity_name
    def apply(op, db):
        db._remove_entity_(op.entity_name)

class RenameEntity(Operation):
    def __init__(op, old_name, new_name, **kw):
        Operation.__init__(op, **kw)
        op.old_name = old_name
        op.new_name = new_name
    def apply(op, db):
        db._rename_entity_(op.old_name, op.new_name)

def _clone_attr(attr):
    new_attr = object.__new__(attr.__class__)
    for cls in attr.__class__.__mro__:
        for slot in getattr(cls, '__slots__', ()):
            value = getattr(attr, slot, None)
            setattr(new_attr, slot, deepcopy(value))
    if hasattr(attr, '__dict__'):
        new_attr.__dict__.update(attr.__dict__)
    new_attr.entity = attr.entity
    return new_attr

class AddAttr(Operation):
    def __init__(op, entity_name, attr_name, attr, **kw):
        Operation.__init__(op, **kw)
        op.entity_name = entity_name
        op.attr_name = attr_name
        op.attr = attr
    def apply(op, db):
        entity = db.entities[op.entity_name]
        entity._add_attr_(op.attr_name, _clone_attr(op.attr))

class RemoveAttr(Operation):
    def __init__(op, entity_name, attr_name, **kw):
        Operation.__init__(op, **kw)
        op.entity_name = entity_name
        op.attr_name = attr_name
    def apply(op, db):
        entity = db.entities[op.entity_name]
        entity._remove_attr_(op.attr_name)

class RenameAttr(Operation):
    def __init__(op, entity_name, old_name, new_name, **kw):
        Operation.__init__(op, **kw)
        op.entity_name = entity_name
        op.old_name = old_name
        op.new_name = new_name
    def apply(op, db):
        entity = db.entities[op.entity_name]
        entity._rename_attr_(op.old_name, op.new_name)

class ModifyAttr(Operation):
    def __init__(op, entity_name, attr_name, new_attr, **kw):
        Operation.__init__(op, **kw)
        op.entity_name = entity_name
        op.attr_name = attr_name
        op.new_attr = new_attr
    def apply(op, db):
        entity = db.entities[op.entity_name]
        entity._modify_attr_(op.attr_name, _clone_attr(op.new_attr))

class AddRelation(Operation):
    def __init__(op, entity_name, attr_name, attr, reverse_name, reverse_attr, **kw):
        Operation.__init__(op, **kw)
        op.entity_name = entity_name
        op.attr_name = attr_name
        op.attr = attr
        op.reverse_name = reverse_name
        op.reverse_attr = reverse_attr

    def _get_rentity(op, db):
        rentity = op.attr.py_type
        if isfunction(rentity):
            rentity = rentity()
        elif isinstance(rentity, basestring):
            return db.entities[rentity]

        return db.entities[rentity.__name__]

    def apply(op, db):
        entity = db.entities[op.entity_name]
        rentity = op._get_rentity(db)
        attr1 = _clone_attr(op.attr)
        attr2 = _clone_attr(op.reverse_attr)
        attr1.py_type = rentity
        attr1.reverse = op.reverse_name
        attr2.py_type = entity
        attr2.reverse = op.attr_name
        entity._add_relation_(op.attr_name, attr1, rentity, op.reverse_name, attr2)

class RemoveRelation(Operation):
    def __init__(op, entity_name, attr_name, **kw):
        Operation.__init__(op, **kw)
        op.entity_name = entity_name
        op.attr_name = attr_name
    def apply(op, db):
        entity = db.entities[op.entity_name]
        entity._remove_relation_(op.attr_name)

class ModifyRelation(Operation):
    def __init__(op, entity_name, attr_name, attr, reverse_name, reverse_attr, **kw):
        Operation.__init__(op, **kw)
        op.entity_name = entity_name
        op.attr_name = attr_name
        op.attr = attr
        op.reverse_name = reverse_name
        op.reverse_attr = reverse_attr
    def apply(op, db):
        assert False, 'Not implemented'
        # entity = db.entities[op.entity_name]
        # attr = entity._adict_[op.attr_name]
        # attr._modify_relation_(op.new_name, _clone_attr(op.new_attr),
        #                        op.reverse_name, _clone_attr(op.reverse_attr))


class Custom(Operation):
    is_custom = True

    def __init__(op, forward, changes=(), final_state=None):
        op.forward = forward
        op.changes = changes
        op.final_state = final_state

    def apply(op, db):
        if not op.final_state:
            for o in op.changes:
                o.apply(db)
            return
        db.schema = None
        for E_name in db.entities:
            db.__dict__.pop(E_name, None)
        db.entities = {}
        op.final_state(db)

    def __repr__(op):
        name = op.forward.__name__
        return 'Custom: {}'.format(name)