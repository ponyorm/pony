from __future__ import unicode_literals
from pony.py23compat import unicode

from datetime import datetime
from itertools import chain
from operator import attrgetter
from contextlib import contextmanager

import pony
from pony import orm

from . import diagram_ops as ops
from .serializer import serialize, serialize_entity_declaration
from .questioner import InteractiveMigrationQuestioner


def indent(text, prefix):
    lines = text.splitlines()
    new_lines = [''.join((prefix, l)) for l in lines]
    return '\n'.join(new_lines)


def get_entities(db):
    return {entity_name: entity for entity_name, entity in db.entities.items() if entity_name != 'Migration'}


class MigrationWriter(object):

    def __init__(self, deps, db_prev, db):
        self.deps = deps
        self.db_prev = db_prev
        self.db = db
        self.questioner = InteractiveMigrationQuestioner()

    def _get_ops(self):
        result = []
        entities = get_entities(self.db)
        entities_prev = get_entities(self.db_prev)

        eadded = {}
        emodified = {}

        for ename, entity in self.db.entities.items():
            if ename not in self.db_prev.entities:
                eadded[ename] = sorted((a.name, a) for a in entity._new_attrs_)

        eremoved = {}

        for ename, prev_entity in self.db_prev.entities.items():
            if ename not in self.db.entities:
                eremoved[ename] = sorted((a.name, a) for a in prev_entity._new_attrs_)

        entity_renames = {}

        @contextmanager
        def apply_renames(renames):
            for from_, to_ in renames.items():
                entity = entities[to_]
                entity.__name__ = from_
            yield
            for from_, to_ in renames.items():
                entity = entities[to_]
                entity.__name__ = to_

        for rem_ename, rem_attrs in sorted(eremoved.items()):
            for add_ename, add_attrs in sorted(eadded.items()):
                with apply_renames({rem_ename: add_ename}):
                    for (add_aname, add_attr), (rem_aname, rem_attr) in zip(add_attrs, rem_attrs):
                        if add_aname != rem_aname:
                            break

                        if add_attr.get_declaration_id() != rem_attr.get_declaration_id():
                            break
                    else:
                        if self.questioner.ask_rename_model(rem_ename, add_ename):
                            result.append(ops.RenameEntity(rem_ename, add_ename))
                            entity_renames[rem_ename] = add_ename
                            del eadded[add_ename]
                            del eremoved[rem_ename]

        with apply_renames(entity_renames):
            for entity in sorted(self.db.entities.values(), key=attrgetter('_id_')):
                ename = entity.__name__
                if ename == 'Migration' or ename not in entities_prev:
                    continue

                prev_entity = entities_prev[ename]
                aadded, aremoved, amodified = self._get_entity_changes(ename, entity, prev_entity)
                for add_name, add_attr in sorted(aadded.items()):
                    for rem_name, rem_attr in sorted(aremoved.items()):
                        try:
                            add_attr.name = rem_attr.name
                            if add_attr.get_declaration_id() == rem_attr.get_declaration_id() \
                            and self.questioner.ask_rename(ename, rem_name, add_name):
                                result.append(ops.RenameAttr(ename, rem_name, add_name))
                                del aremoved[rem_name]
                                del aadded[add_name]
                        finally:
                            add_attr.name = add_name
                emodified[ename] = aadded, aremoved, amodified

        def get_id(pair):
            return tuple(
                tuple(a and a.get_declaration_id() for a in attrs or (None, None))
                for attrs in pair
            )

        def sort_pair(pair):
            result = []
            for attrs in pair:
                if attrs:
                    attr, rattr = attrs
                    if getattr(attr, 'name', '') < getattr(rattr, 'name', ''):
                        attrs = rattr, attr
                result.append(attrs)
            return result

        pairs = {}

        for ename, (aadded, aremoved, amodified) in sorted(emodified.items()):

            # Get entity operations
            for aname, attr in sorted(aadded.items()):
                if not attr.reverse:
                    if issubclass(attr.py_type, str) and isinstance(attr, orm.Optional):
                        kwargs = attr._constructor_args[1]
                        value_class = self.db.provider.sqlbuilder_cls.value_class
                        value = value_class(self.db.provider.paramstyle, '')
                        kwargs.update(sql_default=unicode(value))
                    elif not attr.nullable and attr.initial is None and attr.default is None and not attr.is_pk:
                        initial = self.questioner.ask_not_null_addition(aname, ename)
                        attr.initial = attr._constructor_args[1]['initial'] = initial
                    result.append(ops.AddAttr(ename, aname, attr))
            for aname, attr in sorted(aremoved.items()):
                if not attr.reverse:
                    result.append(ops.RemoveAttr(ename, aname))
            for aname, attr in sorted(amodified.items()):
                if not attr.reverse:
                    attr_prev = self.db_prev.entities[ename]._adict_[aname]
                    result.append(ops.AddAttr(ename, aname, attr) if attr_prev.reverse else ops.ModifyAttr(ename, aname, attr))

            new_entity = entities.get(ename) or entities.get(entity_renames.get(ename))
            assert new_entity is not None
            adict = {a.name: a for a in new_entity._new_attrs_}

            prev_entity = entities_prev[ename]
            prev_adict = {a.name: a for a in prev_entity._new_attrs_}

            for aname in chain(aadded, amodified):
                attr = adict[aname]
                prev_attr = prev_adict.get(aname)
                if prev_attr and not prev_attr.reverse:
                    prev_attr = None
                if attr and not attr.reverse:
                    attr = None
                if not attr and not prev_attr:
                    continue
                if prev_attr:
                    pair = (attr and attr.reverse, attr), (prev_attr, prev_attr.reverse)
                elif attr.reverse.name in prev_adict:
                    attr = attr.reverse
                    prev_attr = getattr(prev_entity, attr.name)
                    pair = (attr, attr.reverse), (prev_attr, prev_attr.reverse)
                else:
                    pair = (attr, attr.reverse), None
                pair = sort_pair(pair)
                pairs[get_id(pair)] = pair

            for aname in aremoved:
                prev_attr = prev_adict[aname]
                if not prev_attr.reverse:
                    prev_attr = None
                attr = adict.get(aname)
                if attr and not attr.reverse:
                    attr = None
                if not attr and not prev_attr:
                    continue

                if attr:
                    pair = (attr, attr.reverse), (prev_attr, prev_attr and prev_attr.reverse)
                else:
                    pair = None, (prev_attr, prev_attr.reverse)

                pair = sort_pair(pair)
                pairs[get_id(pair)] = pair

        def name(attr):
            return attr and attr.name

        def ename(attr):
            return attr.entity.__name__

        for pair, pair_prev in pairs.values():
            attr = attr_prev = None
            if pair_prev:
                attr_prev, rattr_prev = pair_prev
            if pair:
                attr, rattr = pair
            if getattr(attr, 'reverse', None) and getattr(attr_prev, 'reverse', None):
                result.append(ops.ModifyRelation(
                    ename(attr), name(attr), attr, name(rattr), rattr
                ))
            elif getattr(attr, 'reverse', None):
                result.append(ops.AddRelation(
                    ename(attr), name(attr), attr, name(rattr), rattr
                ))
            elif getattr(attr_prev, 'reverse', None):
                result.append(ops.RemoveRelation(ename(attr_prev), attr_prev.name))


        for ename, attrs in sorted(eadded.items(), key=lambda x: entities[x[0]]._id_):
            bases = [c.__name__ for c in entities[ename].__bases__]
            regular = [(k, v) for k, v in attrs if not v.reverse]
            result.append(
                ops.AddEntity(ename, bases, regular)
            )
            for aname, attr in attrs:
                if not attr.reverse:
                    continue
                prev_entity = entities_prev.get(attr.reverse.entity.__name__)
                prev_attr = None
                if prev_entity:
                    prev_attr = prev_entity._adict_.get(attr.reverse.name)
                    if prev_attr.entity is not prev_entity: assert False, 'Not implemented'
                    if prev_attr and not prev_attr.reverse:
                        prev_attr = None
                if prev_attr:
                    pair = (attr, attr.reverse), (prev_attr, prev_attr.reverse)
                else:
                    pair = (attr, attr.reverse), None
                pair = sort_pair(pair)
                iden = get_id(pair)
                if iden not in pairs:
                    pairs[iden] = pair
                    result.append(ops.AddRelation(
                        ename, name(attr), attr, name(attr.reverse), attr.reverse
                    ))
        for ename, attrs in sorted(eremoved.items()):
            result.append(
                ops.RemoveEntity(ename)
            )
            for aname, prev_attr in attrs:
                if not prev_attr.reverse:
                    continue
                entity = entities.get(prev_attr.reverse.entity.__name__)
                attr = None
                if entity:
                    attr = entity._adict_.get(prev_attr.reverse.name)
                    if attr.entity is not entity: assert False, 'Not implemented'
                    if attr and not attr.reverse:
                        attr = None
                if attr:
                    pair = (attr, attr.reverse), (prev_attr, prev_attr.reverse)
                else:
                    pair = None, (prev_attr, prev_attr.reverse)
                pair = sort_pair(pair)
                iden = get_id(pair)
                if iden not in pairs:
                    pairs[iden] = pair
                    result.append(ops.RemoveRelation(ename, aname))

        # sorting
        def keyfunc(op):
            if isinstance(op, ops.RenameAttr):
                return 0
            if isinstance(op, ops.RenameEntity):
                return 1
            if isinstance(op, ops.RemoveRelation):
                return 2
            if 'Remove' in op.__class__.__name__:
                return 3
            if isinstance(op, ops.AddEntity):
                return 4
            return 5

        return sorted(result, key=keyfunc)

    def _get_entity_changes(self, ename, entity, prev_entity):
        added = {}
        modified = {}

        def as_dict(alist):
            return {a.name: a for a in alist}

        E__new_attrs_ = as_dict(entity._new_attrs_)
        E_prev__new_attrs_ = as_dict(prev_entity._new_attrs_)
        for aname, attr in sorted(E__new_attrs_.items()):
            if aname in E_prev__new_attrs_:
                attr_prev = E_prev__new_attrs_[aname]
                if bool(attr.reverse) != bool(attr_prev.reverse):
                    added[aname] = attr
                    continue
                if attr.get_declaration_id() != attr_prev.get_declaration_id():
                    prev_nullable = attr_prev.nullable
                    nullable = attr.nullable
                    if prev_nullable and not nullable:
                        initial = self.questioner.ask_not_null_alteration(aname, ename)
                        attr.initial = attr._constructor_args[1]['initial'] = initial
                    modified[aname] = attr
            else:
                added[aname] = attr

        removed = {}
        for aname, prev_attr in sorted(E_prev__new_attrs_.items()):
            if aname in E__new_attrs_:
                attr = getattr(entity, aname)
                if bool(attr.reverse) == bool(prev_attr.reverse):
                    continue
            removed[aname] = prev_attr

        return added, removed, modified

    def _get_operations_block(self, imports):
        self.operations = self._get_ops()
        lines = ',\n    '.join(serialize(op, imports=imports) for op in self.operations)
        return 'operations = [\n    %s]' % lines

    def _get_define_entities_block(self, imports, func_name='define_entities'):
        entities = []

        for entity in sorted(self.db.entities.values(), key=attrgetter('_id_')):
            if entity.__name__ != 'Migration':
                entities.append(serialize_entity_declaration(entity, imports=imports))

        if not entities:
            entities.append('pass')

        entities = '\n\n'.join(entities)
        entities = indent(entities, '    ')

        # remove spaces
        entities = '\n'.join(
            '' if not line.strip() else line
            for line in entities.splitlines()
        )
        define = 'def %s(db):' % func_name
        return '\n'.join((define, entities))

    def as_string(self):
        ctx = {
            'version': pony.__version__,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M"),
            'deps': '[%s]' % ', '.join(repr(migration.name).lstrip('u') for migration in self.deps),
        }
        imports = {'from pony import orm'}
        if self.db_prev is None:
            body = self._get_define_entities_block(imports)
        else:
            body = self._get_operations_block(imports)

        ctx['body'] = body
        database = serialize(self.db, imports=imports)
        ctx['database'] = database
        ctx['imports'] = sorted(imports, key=lambda i: i.split()[1])
        ctx['imports'] = '\n'.join(ctx['imports'])
        return MIGRATION_TEMPLATE.format(**ctx)


MIGRATION_TEMPLATE = """\
# -*- coding: utf-8 -*-
# Generated by Pony ORM {version} on {timestamp}
from __future__ import unicode_literals

{imports}

dependencies = {deps}

{body}
"""
