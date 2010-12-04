from compiler import ast
from types import NoneType

from pony import orm
from pony.decompiler import decompile
from pony.templating import Html, StrHtml
from pony.dbapiprovider import SQLBuilder
from pony.sqlsymbols import *

class TranslationError(Exception): pass

def select(gen):
    tree, param_names = decompile(gen)
    globals = gen.gi_frame.f_globals
    locals = gen.gi_frame.f_locals
    translator = SQLTranslator(tree, globals, locals)
    values = {}
    for name in translator.params:
        try: value = locals[name]
        except KeyError: value = globals[name]
        values[name] = value
    entity = translator.entity
    sql_ast = translator.sql_ast
    objects = entity._find_by_ast_(sql_ast, values, translator.attr_offsets)
    return objects

primitive_types = set([ int, unicode ])
type_normalization_dict = { long : int, str : unicode, StrHtml : unicode, Html : unicode }

def normalize_type(t):
    if t is NoneType: return t
    t = type_normalization_dict.get(t, t)
    if t not in primitive_types and not isinstance(t, orm.EntityMeta): raise TypeError, t
    return t

def is_comparable_types(op, t1, t2):
    return normalize_type(t1) == normalize_type(t2)

def is_comparable_types(op, type1, type2):
    # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
    #         | 'in' | 'not' 'in' | 'is' | 'is' 'not'
    if op in ('is', 'is not'): return type1 is not NonType and type2 is NoneType
    if op in ('<', '<=', '>', '>='): return type1 is type2 and type1 in primitive_types
    if op in ('==', '<>', '!='):
        if type1 is NoneType and type2 is NoneType: return False
        if type1 is NoneType or type2 is NoneType: return True
        elif type1 in primitive_types: return type1 is type2
        elif isinstance(type1, orm.EntityMeta): return type1._root_ is type2._root_
        else: return False
    if op in ['in', 'not in']:
        if type1 in primitive_types:            
            if type2 is list: return True
            elif isinstance(type2, orm.Set): raise NotImplementedError
            else: return False
        elif isinstance(type1, orm.EntityMeta):
            if type2 is list: return True
            elif isinstance(type2, orm.Set):
                t = type2.py_type
                return isinstance(t, orm.EntityMeta) and type1._root_ is t._root_
            else: return False
        else: return False

def sqland(items):
    if len(items) == 1: return items[0]
    return [ AND ] + items

def sqlor(items):
    if len(items) == 1: return items[0]
    return [ OR ] + items

class ASTTranslator(object):
    def __init__(self, tree):
        self.tree = tree
        self.pre_methods = {}
        self.post_methods = {}
    def dispatch(self, node):
        cls = node.__class__

        try: pre_method = self.pre_methods[cls]
        except KeyError:
            pre_method = getattr(self, 'pre' + cls.__name__, None)
            self.pre_methods[cls] = pre_method
        if pre_method is not None:
            # print 'PRE', node.__class__.__name__, '+'
            pre_method(node)
        else:            
            # print 'PRE', node.__class__.__name__, '-'
            self.default_pre(node)
        
        for child in node.getChildNodes(): self.dispatch(child)

        try: post_method = self.post_methods[cls]
        except KeyError:
            post_method = getattr(self, 'post' + cls.__name__, None)
            self.post_methods[cls] = post_method
        if post_method is not None:
            # print 'POST', node.__class__.__name__, '+'
            post_method(node)
        else:            
            # print 'POST', node.__class__.__name__, '-'
            self.default_post(node)
    def default_pre(self, node):
        pass
    def default_post(self, node):
        pass

class SQLTranslator(ASTTranslator):
    def __init__(self, tree, globals, locals={}):
        assert isinstance(tree, ast.GenExprInner)
        ASTTranslator.__init__(self, tree)
        self.diagram = None
        self.locals = locals
        self.globals = globals
        self.iterables = iterables = {}
        self.aliases = aliases = {}
        self.params = set()
        self.from_ = [ FROM ]
        self.conditions = []
        
        for qual in tree.quals:
            assign = qual.assign
            if not isinstance(assign, ast.AssName): raise TypeError
            if assign.flags != 'OP_ASSIGN': raise TypeError

            name = assign.name
            if name in iterables: raise TranslationError('Duplicate name: %s' % name)
            if name.startswith('__'): raise TranslationError('Illegal name: %s' % name)
            assert name not in aliases

            assert isinstance(qual.iter, ast.Name)
            iter_name = qual.iter.name
            try: value = locals[iter_name]
            except KeyError: value = globals[iter_name] # can raise KeyError
            if isinstance(value, orm.EntityIter): entity = value.entity
            elif isinstance(value, orm.EntityMeta): entity = value
            else: raise NotImplementedError

            if self.diagram is None: self.diagram = entity._diagram_
            elif self.diagram is not entity._diagram_: raise TranslationError(
                'All entities in a query must belong to the same diagram')

            table = entity._table_
            iterables[name] = entity
            aliases[name] = entity
            self.from_.append([ name, TABLE, table ])
            for if_ in qual.ifs:
                assert isinstance(if_, ast.GenExprIf)
                self.dispatch(if_)
                self.conditions.append(if_.monad.getsql())
        assert isinstance(tree.expr, ast.Name)
        alias = self.alias = tree.expr.name
        self.dispatch(tree.expr)
        monad = tree.expr.monad
        entity = self.entity = monad.type
        assert isinstance(entity, orm.EntityMeta)
        self.select, self.attr_offsets = entity._construct_select_clause_(alias)         
        self.sql_ast = [ SELECT, self.select, self.from_ ]
        if self.conditions: self.sql_ast.append([ WHERE, sqland(self.conditions) ])

    def postGenExprIf(self, node):
        monad = node.test.monad
        if monad.type is not bool: raise TypeError
        node.monad = monad
    def postCompare(self, node):
        expr1 = node.expr
        ops = node.ops
        if len(ops) > 1: raise NotImplementedError
        op, expr2 = ops[0]
        # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
        #         | 'in' | 'not in' | 'is' | 'is not'
        node.monad = expr1.monad.cmp(op, expr2.monad)
    def postConst(self, node):
        value = node.value
        if type(value) is not tuple: items = (value,)
        monads = []
        for item in items:
            item_type = normalize_type(type(item))
            if item_type is unicode:
                monads.append(StringConstMonad(self, item))
            elif item_type is int:
                monads.append(NumericConstMonad(self, item))
            elif item_type is NoneType:
                monads.append(NoneMonad(self))
            else: raise TypeError
        if type(value) is not tuple: node.monad = monads[0]
        else: node.monad = ListMonad(self, monads)
    def postName(self, node):
        name = node.name
        if name in self.iterables:
            entity = self.iterables[name]
            node.monad = ObjectIterMonad(self, name, entity)
        else:
            try: value = self.locals[name]
            except KeyError:
                try: value = self.globals[name]
                except KeyError: raise NameError(name)
            if value is None: node.monad = NoneMonad
            else: node.monad = ParamMonad(self, type(value), name)
    def postAdd(self, node):
        node.monad = node.left.monad + node.right.monad
    def postSub(self, node):
        node.monad = node.left.monad - node.right.monad
    def postMul(self, node):
        node.monad = node.left.monad * node.right.monad
    def postDiv(self, node):
        node.monad = node.left.monad / node.right.monad
    def postPower(self, node):
        node.monad = node.left.monad ** node.right.monad
    def postUnarySub(self, node):
        node.monad = -node.expr.monad
    def postGetattr(self, node):
        node.monad = node.expr.monad.getattr(node.attrname)
    def postAnd(self, node):
        node.monad = AndMonad([ subnode.monad for subnode in node.nodes ])
    def postOr(self, node):
        node.monad = OrMonad([ subnode.monad for subnode in node.nodes ])
    def postNot(self, node):
        node.monad = NotMonad(node.expr.monad)

class Monad(object):
    def __init__(monad, translator, type):
        monad.translator = translator
        monad.type = type
    def cmp(monad, op, monad2):
        return CmpMonad(op, monad, monad2)
    def __contains__(monad, item): raise TypeError
    def __nonzero__(monad): raise TypeError

    def getattr(monad): raise TypeError
    def __call__(monad, *args, **keyargs): raise TypeError
    def __len__(monad): raise TypeError
    def __getitem__(monad, key): raise TypeError
    def __iter__(monad): raise TypeError

    def __add__(monad, monad2): raise TypeError
    def __sub__(monad, monad2): raise TypeError
    def __mul__(monad, monad2): raise TypeError
    def __div__(monad, monad2): raise TypeError
    def __pow__(monad, monad2): raise TypeError

    def __neg__(monad): raise TypeError
    def __abs__(monad, monad2): raise TypeError

class NoneMonad(Monad):
    def __init__(monad, translator):
        Monad.__init__(monad, translator, NoneType)
    def getsql(monad):
        return [[ VALUE, None ]]

class ListMonad(Monad):
    def __init__(monad, translator, items):
        Monad.__init__(monad, translator, list)
        monad.items = items

def make_numeric_binop(sqlop):
    def numeric_binop(monad, monad2):
        if not isinstance(monad2, NumericMixin): raise TypeError
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return ExprMonad(monad.translator, int, [ sqlop, left_sql[0], right_sql[0] ])
    numeric_binop.__name__ = sqlop
    return numeric_binop

class NumericMixin(object):
    __add__ = make_numeric_binop(ADD)
    __sub__ = make_numeric_binop(SUB)
    __mul__ = make_numeric_binop(MUL)
    __div__ = make_numeric_binop(DIV)
    __pow__ = make_numeric_binop(POW)
    def __neg__(monad):
        sql = monad.getsql()
        assert len(sql) == 1
        return ExprMonad(monad.translator, int, [ NEG, sql[0] ])

class StringMixin(object): pass
class ObjectMixin(object): pass

class ObjectIterMonad(ObjectMixin, Monad):
    def __init__(monad, translator, alias, entity):
        Monad.__init__(monad, translator, entity)
        monad.alias = alias
    def getattr(monad, name):
        entity = monad.type
        attr = getattr(entity, name) # can raise AttributeError
        return AttrMonad(monad.translator, attr, monad.alias)
    def getsql(monad):
        entity = monad.type
        return [ [ COLUMN, monad.alias, column ] for attr in entity._pk_attrs_ if not attr.is_collection
                                                 for column in attr.columns ]

class AttrMonad(Monad):
    def __new__(cls, translator, attr, *args, **keyargs):
        assert cls is AttrMonad
        type = normalize_type(attr.py_type)
        if type is int: cls = NumericAttrMonad
        elif type is unicode: cls = StringAttrMonad
        elif isinstance(type, orm.EntityMeta): cls = ObjectAttrMonad
        else: assert False
        return object.__new__(cls)
    def __init__(monad, translator, attr, base_alias, columns=None, alias=None):
        type = normalize_type(attr.py_type)
        Monad.__init__(monad, translator, type)
        monad.attr = attr
        monad.base_alias = base_alias
        monad.columns = columns or attr.columns
        monad.alias = alias or '-'.join((base_alias, attr.name))
    def getsql(monad):
        return [ [ COLUMN, monad.base_alias, column ] for column in monad.columns ]

class ObjectAttrMonad(ObjectMixin, AttrMonad):
    def getattr(monad, name):
        translator = monad.translator
        entity = monad.type
        attr = getattr(entity, name) # can raise AttributeError
        if attr.pk_offset is not None:
            base_alias = monad.base_alias
            columns = monad.columns
            if entity._pk_is_composite_:
                i = 0
                for a in entity._pk_attrs_:
                    if a is attr: break
                    i += len(a.columns)
                columns = columns[i:i+len(attr.columns)]
        else:
            alias = monad.translator.aliases.get(monad.alias)
            if alias is None:
                alias = monad.translator.aliases[monad.alias] = monad.alias
                translator.from_.append([ monad.alias, TABLE, entity._table_ ])
                conditions = monad.translator.conditions
                assert len(monad.columns) == len(entity._pk_columns_)
                for c1, c2 in zip(monad.columns, entity._pk_columns_):
                    conditions.append([ EQ, [ COLUMN, monad.base_alias, c1 ], [ COLUMN, monad.alias, c2 ] ])
            base_alias = monad.alias
            columns = attr.columns
        alias = '-'.join((monad.alias, name))
        return AttrMonad(translator, attr, base_alias, columns, alias)

class NumericAttrMonad(NumericMixin, AttrMonad): pass
class StringAttrMonad(StringMixin, AttrMonad): pass

class ParamMonad(Monad):
    def __new__(cls, translator, type, name):
        assert cls is ParamMonad
        type = normalize_type(type)
        if type is int: cls = NumericParamMonad
        elif type is unicode: cls = StringParamMonad
        elif isinstance(type, orm.EntityMeta): cls = ObjectParamMonad
        else: assert False
        return object.__new__(cls)
    def __init__(monad, translator, type, name):
        type = normalize_type(type)
        Monad.__init__(monad, translator, type)
        monad.name = name
        translator.params.add(name)
    def getsql(monad):
        return [ [ PARAM, monad.name ] ]

class ObjectParamMonad(ObjectMixin, ParamMonad):
    def __init__(monad, translator, entity, name):
        if translator.diagram is not entity._diagram_: raise TranslationError(
            'All entities in a query must belong to the same diagram')
        ParamMonad.__init__(monad, translator, entity, name)
        pk_len = len(entity._pk_columns_)
        if pk_len == 1: monad.params = [ name ]
        else: monad.params = [ '%s-%d' % (name, i+1) for i in range(pk_len) ]
        translator.params.update(monad.params)
    def getattr(monad, name):
        raise NotImplementedError
    def getsql(monad):
        return [ [ PARAM, param ] for param in monad.params ]

class StringParamMonad(StringMixin, ParamMonad): pass
class NumericParamMonad(NumericMixin, ParamMonad): pass

class ExprMonad(Monad):
    def __new__(cls, translator, type, sql):
        assert cls is ExprMonad
        type = normalize_type(type)
        if type is int: cls = NumericExprMonad
        elif type is unicode: cls = StringExprMonad
        else: assert False
        return object.__new__(cls)        
    def __init__(monad, translator, type, sql):
        Monad.__init__(monad, translator, type)
        monad.sql = sql
    def getsql(monad):
        return [ monad.sql ]

class StringExprMonad(StringMixin, ExprMonad): pass
class NumericExprMonad(NumericMixin, ExprMonad): pass

class ConstMonad(Monad):
    def __init__(monad, translator, value):
        value_type = normalize_type(type(value))
        Monad.__init__(monad, translator, value_type)
        monad.value = value
    def getsql(monad):
        return [ [ VALUE, monad.value ] ]

class StringConstMonad(StringMixin, ConstMonad): pass
class NumericConstMonad(NumericMixin, ConstMonad): pass

class BoolMonad(Monad):
    def __init__(monad, translator):
        monad.translator = translator
        monad.type = bool

cmpops = { '>=' : GE, '>' : GT, '<=' : LE, '<' : LT }        

class CmpMonad(BoolMonad):
    def __init__(monad, op, left, right):
        if not is_comparable_types(op, left.type, right.type): raise TypeError, [left.type, right.type]
        if op == '<>': op = '!='
        if left.type is NoneType:
            assert right.type is not NoneType
            left, right = right, left
        if right.type is NoneType:
            if op == '==': op = 'is'
            elif op == '!=': op = 'is not'
        elif op == 'is': op = '=='
        elif op == 'is not': op = '!='
        BoolMonad.__init__(monad, left.translator)
        monad.op = op
        monad.left = left
        monad.right = right
    def getsql(monad):
        op = monad.op
        sql = []
        left_sql = monad.left.getsql()
        if op == 'is':
            return sqland([ [ IS_NULL, item ] for item in left_sql ])
        if op == 'is not':
            return sqland([ [ IS_NOT_NULL, item ] for item in left_sql ])
        right_sql = monad.right.getsql()
        assert len(left_sql) == len(right_sql)
        if op in ('<', '<=', '>', '>='):
            assert len(left_sql) == len(right_sql) == 1
            return [ cmpops[op], left_sql[0], right_sql[0] ]
        if op == '==':
            return sqland([ [ EQ, a, b ] for (a, b) in zip(left_sql, right_sql) ])
        if op == '!=':
            return sqlor([ [ NE, a, b ] for (a, b) in zip(left_sql, right_sql) ])
        
        if isinstance(monad.right, ListMonad):
            left_type = normalize_type(monad.left)
            for item in monad.right.items:
                if not is_comparable_types(left_type, item.type): raise TypeError
            if len(left_sql) == 1:
                if op == 'in': return [ IN, left_sql[0], right_sql ]
                elif op == 'not in': return [ NOT_IN, left_sql[0], right_sql ]
                else: assert False
            else:
                if op == 'in':
                    return sqlor([ sqland([ [ EQ, a, b ]  for a, b in zip(left_sql, item_sql) ]) for item_sql in right_sql ])
                if op == 'not in':
                    return sqland([ sqlor([ [ NE, a, b ]  for a, b in zip(left_sql, item_sql) ]) for item_sql in right_sql ])

        raise NotImplementedError

class LogicalBinOpMonad(BoolMonad):
    def __init__(monad, operands):
        assert len(operands) >= 2
        for operand in operands:
            if operand.type is not bool: raise TypeError
        BoolMonad.__init__(monad, operands[0].translator)
        monad.operands = operands
    def getsql(monad):
        return [ monad.binop ] + [ operand.getsql() for operand in monad.operands ]

class AndMonad(LogicalBinOpMonad):
    binop = AND

class OrMonad(LogicalBinOpMonad):
    binop = OR

class NotMonad(BoolMonad):
    def __init__(monad, operand):
        if operand.type is not bool: raise TypeError
        BoolMonad.__init__(monad, operand.translator)
        monad.operand = operand
    def getsql(monad):
        return [ NOT, monad.operand.getsql() ]
