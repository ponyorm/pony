from compiler import ast
from types import NoneType

from pony import orm3 as orm
from pony.decompiler import decompile
from pony.templating import Html, StrHtml
from pony.dbapiprovider import SQLBuilder
from pony.sqlsymbols import *

def select(gen):
    tree = decompile(gen).code
    globals = gen.gi_frame.f_globals
    locals = gen.gi_frame.f_locals
    translator = SQLTranslator(tree, globals, locals)
    builder = SQLBuilder(translator.sql_ast)
    print builder.sql

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
            print 'PRE', node.__class__.__name__, '+'
            pre_method(node)
        else:            
            print 'PRE', node.__class__.__name__, '-'
            self.default_pre(node)
        
        for child in node.getChildNodes(): self.dispatch(child)

        try: post_method = self.post_methods[cls]
        except KeyError:
            post_method = getattr(self, 'post' + cls.__name__, None)
            self.post_methods[cls] = post_method
        if post_method is not None:
            print 'POST', node.__class__.__name__, '+'
            post_method(node)
        else:            
            print 'POST', node.__class__.__name__, '-'
            self.default_post(node)
    def default_pre(self, node):
        pass
    def default_post(self, node):
        pass

class SQLTranslator(ASTTranslator):
    def __init__(self, tree, globals, locals={}):
        assert isinstance(tree, ast.GenExprInner)
        ASTTranslator.__init__(self, tree)
        self.locals = locals
        self.globals = globals
        self.iterables = iterables = {}
        self.params = set()
        self.from_ = [ FROM ]
        self.where = []
        
        for qual in tree.quals:
            assign = qual.assign
            if not isinstance(assign, ast.AssName): raise TypeError
            if assign.flags != 'OP_ASSIGN': raise TypeError
            name = assign.name
            if name in iterables: raise SyntaxError
            assert isinstance(qual.iter, ast.Name)
            iter_name = qual.iter.name
            try: value = locals[iter_name]
            except KeyError: value = globals[iter_name] # can raise KeyError
            if not isinstance(value, orm.EntityIter): raise NotImplementedError
            entity = value.entity
            table = entity._table_
            iterables[name] = [ entity ]
            self.from_.append([ name, TABLE, table ])
            for if_ in qual.ifs:
                assert isinstance(if_, ast.GenExprIf)
                self.dispatch(if_)
        self.dispatch(tree.expr)
        self.select = [ ALL ] + tree.expr.monad.getsql('WHERE')
        self.sql_ast = [ SELECT, self.select, self.from_ ]
        if self.where: self.sql_ast.append(self.where)

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
            if value_type is unicode:
                monads.append(StringMonad(self, item))
            elif value_type is int:
                monads.append(NumericMonad(self, item))
            elif value_type is NoneType:
                monads.append(NoneMonad(self))
            elif isinstance(item_type, orm.EntityMeta):
                monads.append(ObjectParamMonad(self, value))
            else: raise TypeError
        if type(value) is not tuple: node.monad = monads[0]
        else: node.monad = ListMonad(self, monads)
    def postName(self, node):
        name = node.name
        if name in self.iterables:
            entity = self.iterables[name][0]
            node.monad = ObjectIterMonad(self, name, entity)
        else:
            try: value = self.locals[name]
            except KeyError:
                try: value = self.globals[name]
                except KeyError: raise NameError(name)
            value_type = normalize_type(type(value))
            if value_type is unicode:
                node.monad = StringParamMonad(self, name)
            elif value_type is int:
                node.monad = NumericParamMonad(self, name)
            elif value_type is NoneType:
                node.monad = NoneMonad(self)
            elif isinstance(value_type, orm.EntityMeta):
                node.monad = ObjectParamMonad(self, name, value_type)
            else: assert False
    def postGetattr(self, node):
        node.monad = node.expr.monad.getattr(node.attrname)
        
class Monad(object):
    def __init__(monad, translator, type):
        monad.translator = translator
        monad.type = type
    def getsql(monad, section):
        raise NotImplementedError
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

    def __neg__(monad): raise TypeError
    def __abs__(monad, monad2): raise TypeError

class NoneMonad(Monad):
    def __init__(monad, translator):
        Monad.__init__(monad, translator, NoneType)
    def getsql(monad, section):
        return [[ VALUE, None ]]

class ListMonad(Monad):
    def __init__(monad, translator, items):
        Monad.__init__(monad, translator, list)
        monad.items = items

class StringMonad(Monad):
    def __init__(monad, translator):
        Monad.__init__(monad, translator, unicode)

class StringParamMonad(Monad):
    def __init__(monad, translator, name):
        StringMonad.__init__(monad, translator)
        monad.name = name
        translator.params.add(name)
    def getsql(monad, section):
        return [ [ PARAM, monad.name ] ]

class StringConstMonad(StringMonad):
    def __init__(monad, translator, value):
        StringMonad.__init__(monad, translator)
        monad.value = value
    def getsql(monad, section):
        return [ [ VALUE, monad.value ] ]

class StringAttrMonad(StringMonad):
    def __init__(monad, parent, attr):
        assert issubclass(attr.py_type, basestring)
        StringMonad.__init__(monad, parent.translator)
        monad.parent = parent
        monad.attr = attr

class NumericMonad(Monad):
    pass

class NumericParamMonad(NumericMonad):
    def __init__(monad, translator, name):
        NumericMonad.__init__(monad, translator, int)
        monad.name = name
        translator.params.add(name)
    def getsql(monad, section):
        return [ [ PARAM, monad.name ] ]

class NumericConstMonad(NumericMonad):
    def __init__(monad, translator, value):
        NumericMonad.__init__(monad, translator, int)
        monad.value = value
    def getsql(monad, section):
        return [ [ VALUE, monad.value ] ]

class NumericAttrMonad(NumericMonad):
    def __init__(monad, parent, attr):
        assert attr.py_type is int
        StringMonad.__init__(monad, parent.translator)
        monad.parent = parent
        monad.attr = attr

class ObjectMonad(Monad):
    def __init__(monad, translator, entity):
        Monad.__init__(monad, translator, entity)

class ObjectIterMonad(ObjectMonad):
    def __init__(monad, translator, alias, entity):
        ObjectMonad.__init__(monad, translator, entity)
        monad.alias = alias
    def getattr(monad, name):
        entity = monad.type
        attr = getattr(entity, name) # can raise AttributeError
        attr_type = normalize_type(attr.py_type)
        if attr_type is int:
            return NumericAttrMonad(monad, attr)
        elif attr_type is unicode:
            return StringAttrMonad(monad, attr)
        elif isinstance(attr_type, orm.EntityMeta):
            return ObjectAttrMonad(monad, attr)
        else: assert False
    def getsql(monad, section):
        entity = monad.type
        alias = monad.alias
        result = []
        if section == 'SELECT': attrs = entity._attrs_
        elif section == 'WHERE': attrs = entity._pk_attrs_
        else: assert False
        for attr in entity._attrs_:
            if attr.is_collection: continue
            for column in attr.get_columns():
                result.append([ COLUMN, alias, column ])
        return result
        
class ObjectParamMonad(ObjectMonad):
    def __init__(monad, translator, name, entity):
        ObjectMonad.__init__(monad, translator, entity)
        monad.name = name
        pk_columns = entity._get_pk_columns_()
        if len(pk_columns) == 1:
            translator.params.add(name)
        else:
            prefix = name + '$'
            for column in pk_columns:
                param_name = prefix + column
                translator.params.add(param_name)
    def getattr(monad, name):
        raise NotImplementedError
    def getsql(monad, section):
        raise TypeError

class ObjectAttrMonad(ObjectMonad):
    def __init__(monad, parent, attr):
        assert isinstance(attr.py_type, orm.EntityMeta)
        if not attr.is_collection: type = attr.py_type
        else: type = attr
        ObjectMonad.__init__(monad, parent.translator, type)
        monad.parent = parent
        monad.attr = attr
    def getsql(monad, section):
        raise NotImplementedError
        
class BoolMonad(Monad):
    def __init__(monad, translator):
        monad.translator = translator
        monad.type = bool

def sqland(items):
    if len(items) == 1: return items[0]
    return [ AND ] + items

def sqlor(items):
    if len(items) == 1: return items[0]
    return [ OR ] + items

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
    def getsql(monad, section):
        sql = []
        left_sql = monad.left.getsql()
        if op == 'is':
            return [ sqland([ [ IS_NULL, item ] for item in left_sql ]) ]
        if op == 'is not':
            return [ sqland([ [ IS_NOT_NULL, item ] for item in left_sql ]) ]
        right_sql = monad.right.get_sql()
        assert len(left_sql) == len(right_sql)
        if op in ('<', '<=', '>', '>='):
            return [ [ cmpops[op], left_sql, right_sql ] ]
        if op == '==':
            return [ sqland([ [ EQ, a, b ] for (a, b) in zip(left_sql, right_sql) ]) ]
        if op == '!=':
            return [ sqlor([ [ NE, a, b ] for (a, b) in zip(left_sql, right_sql) ]) ]
        
        if isinstance(monad.right, ListMonad):
            left_type = normalize_type(monad.left)
            for item in monad.right.items:
                if not is_comparable_types(left_type, item.type): raise TypeError
            if len(left_sql) == 1:
                if op == 'in': return [ [ IN, left_sql[0], right_sql ] ]
                elif op == 'not in': return [ [ NOT_IN, left_sql[0], right_sql ] ]
                else: assert False
            else:
                if op == 'in':
                    return [ sqlor([ sqland([ [ EQ, a, b ]  for a, b in zip(left_sql, item_sql) ]) for item_sql in right_sql ]) ]
                if op == 'not in':
                    return [ sqland([ sqlor([ [ NE, a, b ]  for a, b in zip(left_sql, item_sql) ]) for item_sql in right_sql ]) ]

        raise NotImplementedError

class AndMonad(BoolMonad):
    def __init__(monad, operands):
        assert len(operands) >= 2
        BoolMonad.__init__(monad, operands[0].translator)
        monad.operands = operands

class OrMonad(BoolMonad):
    def __init__(monad, operands):
        assert len(operands) >= 2
        BoolMonad.__init__(monad, operands[0].translator)
        monad.operands = operands

class NotMonad(BoolMonad):
    def __init__(monad, operand):
        BoolMonad.__init__(monad, operand.translator)
        monad.operand = operand
