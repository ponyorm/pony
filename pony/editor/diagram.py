
from pony.py23compat import basestring
from decimal import Decimal
from .placing import Placing


class Diagram:

    def __init__(self, db):
        self.entities = db.entities
        self.db = db
    
    def populate_rel(self, attr, dic):
        dic['rattr'] = attr.reverse.name
        if attr.cascade_delete is not None:
            dic['cascadeDelete'] = attr.cascade_delete

    def populate_attr(self, attr, dic):
        dic['auto'] = attr.auto
        dic['column'] = attr.column
        if attr.default is not None and not callable(attr.default):
            dic['defaultValue'] = attr.default
        if isinstance(attr.py_type, Decimal):
            converter = attr.converters[0]
            dic.update({
                'scale': converter.scale,
                'precision': converter.precision,
            })
            return
        if attr.py_type == str:
            converter = attr.converters[0]
            dic['maxLength'] = converter.max_len

    def build(self):
        ret = {
            'entities': [],
            'state': {
                'scrollTop': 0,
                'scrollLeft': 0,
            },
        }
        for e_name, E in self.entities.items():
            bases = [
                b for b in E.__bases__
                if b.__name__ in self.entities
            ]
            entity = {
                'name': e_name,
                'attrs': [],
                'inheritsFrom': [b.__name__ for b in bases],
            }
            for attr in E._attrs_:
                skip = False
                for b in bases:
                    if attr.name in b._adict_:
                        skip = True
                        break
                if skip:
                    continue
                dic = {}
                py_type = attr.py_type
                if isinstance(py_type, basestring):
                    assert py_type in self.entities
                    py_type = self.entities[py_type]
                dic['type'] = py_type.__name__
                dic['cls'] = type(attr).__name__
                dic['name'] = attr.name
                if attr.nullable is not None:
                    dic['nullable'] = attr.nullable
                if attr.hidden is not None:
                    dic['hidden'] = attr.hidden
                if attr.is_unique:
                    dic['unique'] = attr.is_unique
                if attr.reverse:
                    self.populate_rel(attr, dic)
                else:
                    self.populate_attr(attr, dic)
                entity['attrs'].append(dic)
            ret['entities'].append(entity)
        
        entities = ret['entities']
        placing = Placing(entities)
        placing.apply()
        
        if entities:
            ret['state'].update({
                'scrollTop': entities[0]['top'],
                'scrollLeft': entities[0]['left'],
            })
        return ret


def db_to_diagram(db):
    dia = Diagram(db)
    return dia.build()