from pony.py23compat import iteritems

import json
from datetime import date, datetime
from decimal import Decimal
from collections import defaultdict

from pony.orm.core import Entity, TransactionError
from pony.utils import cut_traceback, throw

class Bag(object):
    def __init__(bag, database):
        bag.database = database
        bag.session_cache = None
        bag.entity_configs = {}
        bag.objects = defaultdict(set)
        bag.vars = {}
        bag.dicts = defaultdict(dict)
    @cut_traceback
    def config(bag, entity, only=None, exclude=None, with_collections=True, with_lazy=False, related_objects=True):
        if bag.database.entities.get(entity.__name__) is not entity: throw(TypeError,
            'Entity %s does not belong to database %r' % (entity.__name__, bag.database))
        attrs = entity._get_attrs_(only, exclude, with_collections, with_lazy)
        bag.entity_configs[entity] = attrs, related_objects
        return attrs, related_objects
    @cut_traceback
    def put(bag, x):
        if isinstance(x, Entity):
            bag._put_object(x)
        else:
            try: x = list(x)
            except: throw(TypeError, 'Entity instance or a sequence of instances expected. Got: %r' % x)
            for item in x:
                if not isinstance(item, Entity): throw(TypeError,
                    'Entity instance or a sequence of instances expected. Got: %r' % item)
                bag._put_object(item)
    def _put_object(bag, obj):
        entity = obj.__class__
        if bag.database.entities.get(entity.__name__) is not entity: throw(TypeError,
            'Entity %s does not belong to database %r' % (entity.__name__, bag.database))
        cache = bag.session_cache
        if cache is None: cache = bag.session_cache = obj._session_cache_
        elif obj._session_cache_ is not cache: throw(TransactionError,
            'An attempt to mix objects belonging to different transactions')
        bag.objects[entity].add(obj)
    def _reduce_composite_pk(bag, pk):
        return ','.join(str(item).replace('*', '**').replace(',', '*,') for item in pk)
    @cut_traceback
    def to_dict(bag):
        bag.dicts.clear()
        for entity, objects in iteritems(bag.objects):
            for obj in objects:
                dicts = bag.dicts[entity]
                if obj not in dicts: bag._process_object(obj)
        result = defaultdict(dict)
        for entity, dicts in iteritems(bag.dicts):
            composite_pk = len(entity._pk_columns_) > 1
            for obj, d in iteritems(dicts):
                pk = obj._get_raw_pkval_()
                if composite_pk: pk = bag._reduce_composite_pk(pk)
                else: pk = pk[0]
                result[entity.__name__][pk] = d
        bag.dicts.clear()
        return result
    def _process_object(bag, obj, process_related=True):
        entity = obj.__class__
        try: attrs, related_objects = bag.entity_configs[entity]
        except KeyError: attrs, related_objects = bag.config(entity)
        process_related_objects = process_related and related_objects
        d = {}
        for attr in attrs:
            value = attr.__get__(obj)
            if attr.is_collection:
                if not process_related:
                    continue
                if process_related_objects:
                    for related_obj in value:
                        if related_obj not in bag.dicts:
                            bag._process_object(related_obj, process_related=False)
                if attr.reverse.entity._pk_is_composite_:
                    value = sorted(bag._reduce_composite_pk(item._get_raw_pkval_()) for item in value)
                else: value = sorted(item._get_raw_pkval_()[0] for item in value)
            elif attr.is_relation:
                if value is not None:
                    if process_related_objects:
                        bag._process_object(value, process_related=False)
                    value = value._get_raw_pkval_()
                    if len(value) == 1: value = value[0]
            d[attr.name] = value
        bag.dicts[entity][obj] = d
    @cut_traceback
    def to_json(bag):
        return json.dumps(bag.to_dict(), default=json_converter, indent=2, sort_keys=True)

def to_dict(objects):
    if isinstance(objects, Entity): objects = [ objects ]
    objects = iter(objects)
    try: first_object = next(objects)
    except StopIteration: return {}
    if not isinstance(first_object, Entity): throw(TypeError,
        'Entity instance or a sequence of instances expected. Got: %r' % first_object)
    database = first_object._database_
    bag = Bag(database)
    bag.put(first_object)
    bag.put(objects)
    return dict(bag.to_dict())

def to_json(objects):
    return json.dumps(to_dict(objects), default=json_converter, indent=2, sort_keys=True)

def json_converter(x):
    if isinstance(x, (datetime, date, Decimal)):
        return str(x)
    raise TypeError(x)
