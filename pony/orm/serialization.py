from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pony.orm.core import Database, Entity, TransactionError
from pony.utils import cut_traceback, throw

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pony.orm.core import Attribute, EntityMeta


class Bag:
    def __init__(self, database: Database) -> None:
        self.database = database
        self.session_cache = None
        self.entity_configs: dict[EntityMeta, tuple[tuple[Attribute, ...], bool]] = {}
        self.objects: defaultdict[EntityMeta, set[Entity]] = defaultdict(set)
        self.vars: dict[Any, Any] = {}
        self.dicts: defaultdict[EntityMeta, dict[Entity, dict[str, Any]]] = defaultdict(
            dict
        )

    @cut_traceback
    def config(
        self,
        entity,
        only=None,
        exclude=None,
        with_collections=True,
        with_lazy=False,
        related_objects=True,
    ):
        if self.database.entities.get(entity.__name__) is not entity:
            throw(
                TypeError,
                "Entity %s does not belong to database %r"
                % (entity.__name__, self.database),
            )
        attrs = entity._get_attrs_(only, exclude, with_collections, with_lazy)
        self.entity_configs[entity] = attrs, related_objects
        return attrs, related_objects

    @cut_traceback
    def put(self, x):
        if isinstance(x, Entity):
            self._put_object(x)
        else:
            try:
                x = list(x)
            except BaseException:
                throw(
                    TypeError,
                    "Entity instance or a sequence of instances expected. Got: %r" % x,
                )
            for item in x:
                if not isinstance(item, Entity):
                    throw(
                        TypeError,
                        "Entity instance or a sequence of instances expected. Got: %r"
                        % item,
                    )
                self._put_object(item)

    def _put_object(self, obj):
        entity = obj.__class__
        if self.database.entities.get(entity.__name__) is not entity:
            throw(
                TypeError,
                "Entity %s does not belong to database %r"
                % (entity.__name__, self.database),
            )
        cache = self.session_cache
        if cache is None:
            cache = self.session_cache = obj._session_cache_
        elif obj._session_cache_ is not cache:
            throw(
                TransactionError,
                "An attempt to mix objects belonging to different transactions",
            )
        self.objects[entity].add(obj)

    def _reduce_composite_pk(self, pk):
        return ",".join(str(item).replace("*", "**").replace(",", "*,") for item in pk)

    @cut_traceback
    def to_dict(self) -> defaultdict[str, dict[Any, dict[str, Any]]]:
        self.dicts.clear()
        for entity, objects in self.objects.items():
            for obj in objects:
                dicts = self.dicts[entity]
                if obj not in dicts:
                    self._process_object(obj)
        result: defaultdict[str, dict[Any, dict[str, Any]]] = defaultdict(dict)
        for entity, dicts in self.dicts.items():
            composite_pk = len(entity._pk_columns_) > 1
            for obj, d in dicts.items():
                pk: Any = obj._get_raw_pkval_()
                if composite_pk:
                    pk = self._reduce_composite_pk(pk)
                else:
                    pk = pk[0]
                result[entity.__name__][pk] = d
        self.dicts.clear()
        return result

    def _process_object(self, obj: Entity, process_related: bool = True) -> None:
        entity = obj.__class__
        try:
            attrs, related_objects = self.entity_configs[entity]
        except KeyError:
            attrs, related_objects = self.config(entity)
        process_related_objects = process_related and related_objects
        d = {}
        for attr in attrs:
            value = attr.__get__(obj)
            if attr.is_collection:
                if not process_related:
                    continue
                if process_related_objects:
                    for related_obj in value:
                        if related_obj not in self.dicts:
                            self._process_object(related_obj, process_related=False)
                if attr.reverse.entity._pk_is_composite_:
                    value = sorted(
                        self._reduce_composite_pk(item._get_raw_pkval_())
                        for item in value
                    )
                else:
                    value = sorted(item._get_raw_pkval_()[0] for item in value)
            elif attr.is_relation:
                if value is not None:
                    if process_related_objects:
                        self._process_object(value, process_related=False)
                    value = value._get_raw_pkval_()
                    if len(value) == 1:
                        value = value[0]
            d[attr.name] = value
        self.dicts[entity][obj] = d

    @cut_traceback
    def to_json(self):
        return json.dumps(
            self.to_dict(), default=json_converter, indent=2, sort_keys=True
        )


def to_dict(
    objects: Entity | Sequence[Entity],
) -> dict[str, dict[Any, dict[str, Any]]]:
    if isinstance(objects, Entity):
        objects = [objects]
    objects_iter = iter(objects)
    try:
        first_object = next(objects_iter)
    except StopIteration:
        return {}
    if not isinstance(first_object, Entity):
        throw(
            TypeError,
            "Entity instance or a sequence of instances expected. Got: %r"
            % first_object,
        )
    database = first_object._database_
    bag = Bag(database)
    bag.put(first_object)
    bag.put(objects_iter)
    return dict(bag.to_dict())


def to_json(objects):
    return json.dumps(
        to_dict(objects), default=json_converter, indent=2, sort_keys=True
    )


def json_converter(x):
    if isinstance(x, (datetime, date, Decimal)):
        return str(x)
    raise TypeError(x)
