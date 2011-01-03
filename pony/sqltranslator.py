import __builtin__
from compiler import ast
from types import NoneType

from pony import orm
from pony.decompiler import decompile
from pony.templating import Html, StrHtml
from pony.dbapiprovider import SQLBuilder
from pony.sqlsymbols import *

class TranslationError(Exception): pass

python_ast_cache = {}
sql_cache = {}

def select(gen):
    tree, external_names = decompile(gen)
    globals = gen.gi_frame.f_globals
    locals = gen.gi_frame.f_locals
    variables = {}
    for name in external_names:
        try: value = locals[name]
        except KeyError:
            try: value = globals[name]
            except KeyError:
                if hasattr(__builtin__, name): continue
                else: raise KeyError, name
        variables[name] = value
    vartypes = dict((name, get_normalized_type(value)) for name, value in variables.iteritems())
    return Query(gen, tree, vartypes, variables)

class Query(object):
    def __init__(query, gen, tree, vartypes, variables):
        query._gen = gen
        query._tree = tree
        query._vartypes = vartypes
        query._variables = variables
        query._result = None
        query._python_ast_key = gen.gi_frame.f_code, tuple(sorted(vartypes.iteritems()))
        translator = python_ast_cache.get(query._python_ast_key)
        if translator is None:
            translator = SQLTranslator(tree, vartypes)
            python_ast_cache[query._python_ast_key] = translator
        query._translator = translator
        query._database = translator.entity._diagram_.database
        query._order = None
        query._limit = None
    def __iter__(query):
        translator = query._translator
        sql_key = query._python_ast_key + (query._order, query._limit)
        cache_entry = sql_cache.get(sql_key)
        database = query._database
        if cache_entry is None:
            sql_ast = translator.sql_ast
            if query._order:
                alias = translator.alias
                orderby_section = [ ORDER_BY ]
                for attr in query._order:
                    for column in attr.columns:
                        orderby_section.append(([COLUMN, alias, column], ASC))
                sql_ast = sql_ast + [ orderby_section ]
            if query._limit:
                start, stop = query._limit
                limit = stop - start
                offset = start
                assert limit is not None
                limit_section = [ LIMIT, [ VALUE, limit ]]
                if offset: limit_section.append([ VALUE, offset ])
                sql_ast = sql_ast + [ limit_section ]
            con, provider = database._get_connection()
            sql, adapter = provider.ast2sql(con, sql_ast)
            cache_entry = sql, adapter
            sql_cache[sql_key] = cache_entry
        else: sql, adapter = cache_entry
        param_dict = {}
        for param_name, extractor in translator.extractors.items():
            param_dict[param_name] = extractor(query._variables)
        arguments = adapter(param_dict)
        cursor = database._exec_sql(sql, arguments)
        objects = translator.entity._fetch_objects(cursor, translator.attr_offsets)
        return iter(objects)
    def orderby(query, *args):
        if not args: raise TypeError('query.orderby() requires at least one argument')
        entity = query._translator.entity
        for arg in args:
            if not isinstance(arg, orm.Attribute): raise TypeError(
                'query.orderby() arguments must be attributes. Got: %r' % arg)
            if entity._adict_.get(arg.name) is not arg: raise TypeError(
                'Attribute %s does not belong to Entity %s' % (arg, entity.__name__))
        new_query = object.__new__(Query)
        new_query.__dict__.update(query.__dict__)
        new_query._order = args
        return new_query
    def __getitem__(query, key):
        if isinstance(key, slice):
            step = key.step
            if step is not None and step <> 1: raise TypeError("Parameter 'step' of slice object is not allowed here")
            start = key.start
            if start is None: start = 0
            elif start < 0: raise TypeError("Parameter 'start' of slice object cannot be negative")
            stop = key.stop
            if stop is None:
                if start is None: return query
                elif not query._limit: raise TypeError("Parameter 'stop' of slice object should be specified")
                else: stop = query._limit[1]
        else:
            try: i = key.__index__()
            except AttributeError:
                try: i = key.__int__()
                except AttributeError:
                    raise TypeError('Incorrect argument type: %r' % key)
            start = i
            stop = i + 1
        if query._limit is not None:
            prev_start, prev_stop = query._limit
            start = prev_start + start
            stop = min(prev_stop, prev_start + stop)
        if start >= stop: start = stop = 0
        new_query = object.__new__(Query)
        new_query.__dict__.update(query.__dict__)
        new_query._limit = start, stop
        return new_query
    def limit(query, limit, offset=None):
        start = offset or 0
        stop = start + limit
        return query[start:stop]

primitive_types = set([ int, unicode ])
type_normalization_dict = { long : int, str : unicode, StrHtml : unicode, Html : unicode }

def get_normalized_type(value):
    if isinstance(value, orm.EntityMeta): return value
    value_type = type(value)
    if value_type is orm.EntityIter: return value.entity
    return normalize_type(value_type)

def normalize_type(t):
    if t is NoneType: return t
    t = type_normalization_dict.get(t, t)
    if t not in primitive_types and not isinstance(t, orm.EntityMeta): raise TypeError, t
    return t

def are_comparable_types(op, type1, type2):
    # op: '<' | '>' | '=' | '>=' | '<=' | '<>' | '!=' | '=='
    #         | 'in' | 'not' 'in' | 'is' | 'is' 'not'
    if op in ('is', 'is not'): return type1 is not NoneType and type2 is NoneType
    if op in ('<', '<=', '>', '>='): return type1 is type2 and type1 in primitive_types
    if op in ('==', '<>', '!='):
        if type1 is NoneType and type2 is NoneType: return False
        if type1 is NoneType or type2 is NoneType: return True
        elif type1 in primitive_types: return type1 is type2
        elif isinstance(type1, orm.EntityMeta): return type1._root_ is type2._root_
        else: return False
    else: assert False

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
    def __init__(self, tree, vartypes):
        assert isinstance(tree, ast.GenExprInner)
        ASTTranslator.__init__(self, tree)
        self.diagram = None
        self.vartypes = vartypes
        self.iterables = iterables = {}
        self.aliases = aliases = {}
        self.extractors = {}
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
            entity = vartypes[iter_name] # can raise KeyError
            if not isinstance(entity, orm.EntityMeta): raise NotImplementedError

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
        if op.endswith('in'):
            node.monad = expr2.monad.contains(expr1.monad, op == 'not in')
        else:
            node.monad = expr1.monad.cmp(op, expr2.monad)
    def postConst(self, node):
        value = node.value
        if type(value) is not tuple:
            node.monad = ConstMonad(self, value)
        else:
            node.monad = ListMonad(self, [ ConstMonad(self, item) for item in value ])
    def postList(self, node):
        node.monad = ListMonad(self, [ item.monad for item in node.nodes ])
    def postTuple(self, node):
        node.monad = ListMonad(self, [ item.monad for item in node.nodes ])
    def postName(self, node):
        name = node.name
        if name in self.iterables:
            entity = self.iterables[name]
            node.monad = ObjectIterMonad(self, name, entity)
        else:
            try: value_type = self.vartypes[name]
            except KeyError:
                func = getattr(__builtin__, name, None)
                if func is None: raise NameError(name)
                func_monad_class = special_functions.get(func)
                if func_monad_class is None: raise NotImlementedError
                node.monad = func_monad_class(self)
            else:
                if value_type is NoneType: node.monad = NoneMonad(self)
                else: node.monad = ParamMonad(self, value_type, name)
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
    def postCallFunc(self, node):
        if node.star_args is not None: raise NotImplementedError
        if node.dstar_args is not None: raise NotImplementedError
        args = []
        keyargs = {}
        for arg in node.args:
            if isinstance(arg, ast.Keyword):
                keyargs[arg.name] = arg.expr.monad
            else: args.append(arg.monad)
        func_monad = node.node.monad
        node.monad = func_monad(*args, **keyargs)
    def postSubscript(self, node):
        assert node.flags == 'OP_APPLY'
        assert isinstance(node.subs, list) and len(node.subs) == 1
        expr_monad = node.expr.monad
        index_monad = node.subs[0].monad
        node.monad = expr_monad[index_monad]
    def postSlice(self, node):
        assert node.flags == 'OP_APPLY'
        expr_monad = node.expr.monad
        upper = node.upper
        if upper is not None: upper = upper.monad
        lower = node.lower
        if lower is not None: lower = lower.monad
        node.monad = expr_monad[lower:upper]

class Monad(object):
    def __init__(monad, translator, type):
        monad.translator = translator
        monad.type = type
    def cmp(monad, op, monad2):
        return CmpMonad(op, monad, monad2)
    def contains(monad, item, not_in=False): raise TypeError
    def __nonzero__(monad): raise TypeError

    def getattr(monad, attrname): raise TypeError
    def __call__(monad, *args, **keyargs): raise TypeError
    def len(monad): raise TypeError
    def __getitem__(monad, key): raise TypeError
    def __iter__(monad): raise TypeError

    def __add__(monad, monad2): raise TypeError
    def __sub__(monad, monad2): raise TypeError
    def __mul__(monad, monad2): raise TypeError
    def __div__(monad, monad2): raise TypeError
    def __pow__(monad, monad2): raise TypeError

    def __neg__(monad): raise TypeError
    def __abs__(monad, monad2): raise TypeError

class ListMonad(Monad):
    def __init__(monad, translator, items):
        Monad.__init__(monad, translator, list)
        monad.items = items
    def contains(monad, x, not_in=False):
        for item in monad.items:
            if not are_comparable_types('==', x.type, item.type): raise TypeError
        left_sql = x.getsql()
        if len(left_sql) == 1:
            if not_in: sql = [ NOT_IN, left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
            else: sql = [ IN, left_sql[0], [ item.getsql()[0] for item in monad.items ] ]
        elif not_in:
            sql = sqland([ sqlor([ [ NE, a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        else:
            sql = sqlor([ sqland([ [ EQ, a, b ]  for a, b in zip(left_sql, item.getsql()) ]) for item in monad.items ])
        return BoolExprMonad(monad.translator, sql)

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

def make_string_binop(sqlop):
    def string_binop(monad, monad2):
        if not isinstance(monad2, StringMixin): raise TypeError
        left_sql = monad.getsql()
        right_sql = monad2.getsql()
        assert len(left_sql) == len(right_sql) == 1
        return ExprMonad(monad.translator, unicode, [ sqlop, left_sql[0], right_sql[0] ])
    string_binop.__name__ = sqlop
    return string_binop

class StringMixin(object):
    def getattr(monad, attrname):
        return StringMethodMonad(monad.translator, monad, attrname)
    __add__ = make_string_binop(CONCAT)
    def __getitem__(monad, index):
        if isinstance(index, slice):
            if index.step is not None: raise TypeError("Slice 'step' attribute is not supported")
            start, stop = index.start, index.stop
            if start is None and stop is None: return monad
            if isinstance(monad, StringConstMonad) \
               and (start is None or isinstance(start, NumericConstMonad)) \
               and (stop is None or isinstance(stop, NumericConstMonad)):
                if start is not None: start = start.value
                if stop is not None: stop = stop.value
                return StringConstMonad(monad.translator, monad.value[start:stop])

            if start is not None and start.type is not int: raise TypeError('string indices must be integers')
            if stop is not None and stop.type is not int: raise TypeError('string indices must be integers')
            
            expr_sql = monad.getsql()[0]

            if start is None:
                start_sql = [ VALUE, 1 ]
            elif isinstance(start, NumericConstMonad):
                if start.value < 0: raise NotImplementedError('Negative slice indices not supported')
                start_sql = [ VALUE, start.value + 1 ]
            else:
                start_sql = start.getsql()[0]
                start_sql = [ ADD, start_sql, [ VALUE, 1 ] ]

            if stop is None:
                len_sql = None
            elif isinstance(stop, NumericConstMonad):
                if stop.value < 0: raise NotImplementedError('Negative slice indices not supported')
                if start is None:
                    len_sql = [ VALUE, stop.value ]
                elif isinstance(start, NumericConstMonad):
                    len_sql = [ VALUE, stop.value - start.value ]
                else:
                    len_sql = [ SUB, [ VALUE, stop.value ], start_sql ]
            else:
                stop_sql = stop.getsql()[0]
                len_sql = [ SUB, stop_sql, start_sql ]

            sql = [ SUBSTR, expr_sql, start_sql, len_sql ]
            return ExprMonad(monad.translator, unicode, sql)
        
        if isinstance(monad, StringConstMonad) and isinstance(index, NumericConstMonad):
            return StringConstMonad(monad.translator, monad.value[index.value])
        if index.type is not int: raise TypeError('string indices must be integers')
        expr_sql = monad.getsql()[0]
        if isinstance(index, NumericConstMonad):
            value = index.value
            if value >= 0: value += 1
            index_sql = [ VALUE, value ]
        else:
            inner_sql = index.getsql()[0]
            index_sql = [ ADD, inner_sql, [ CASE, None, [ ([GE, inner_sql, [ VALUE, 0 ]], [ VALUE, 1 ]) ], [ VALUE, 0 ] ] ]
        sql = [ SUBSTR, expr_sql, index_sql, [ VALUE, 1 ] ]
        return ExprMonad(monad.translator, unicode, sql)
    def len(monad):
        sql = monad.getsql()[0]
        return ExprMonad(monad.translator, int, [ LENGTH, sql ])
        
class MethodMonad(Monad):
    def __init__(monad, translator, parent, attrname):
        Monad.__init__(monad, translator, 'METHOD')
        monad.parent = parent
        monad.attrname = attrname
        try: method = getattr(monad, 'call_' + monad.attrname)
        except AttributeError:
            raise AttributeError('%r object has no attribute %r' % (parent.type.__name__, attrname))
    def __call__(monad, *args, **keyargs):
        method = getattr(monad, 'call_' + monad.attrname)
        return method(*args, **keyargs)

def make_string_func(sqlop):
    def func(monad):
        sql = monad.parent.getsql()
        assert len(sql) == 1
        return ExprMonad(monad.translator, unicode, [ sqlop, sql[0] ])
    func.__name__ = sqlop
    return func

class StringMethodMonad(MethodMonad):
    call_upper = make_string_func(UPPER)
    call_lower = make_string_func(LOWER)
    def call_startswith(monad, arg):
        parent_sql = monad.parent.getsql()[0]
        if arg.type is not unicode:
            raise TypeError("Argument of 'startswith' method must be a string")
        if isinstance(arg, StringConstMonad):
            assert isinstance(arg.value, basestring)
            arg_sql = [ VALUE, arg.value + '%' ]
        else:
            arg_sql = arg.getsql()[0]
            arg_sql = [ CONCAT, arg_sql, [ VALUE, '%' ] ]
        sql = [ LIKE, parent_sql, arg_sql ]
        return BoolExprMonad(monad.translator, sql)
    def call_endswith(monad, arg):
        parent_sql = monad.parent.getsql()[0]
        if arg.type is not unicode:
            raise TypeError("Argument of 'endswith' method must be a string")
        if isinstance(arg, StringConstMonad):
            assert isinstance(arg.value, basestring)
            arg_sql = [ VALUE, '%' + arg.value ]
        else:
            arg_sql = arg.getsql()[0]
            arg_sql = [ CONCAT, [ VALUE, '%' ], arg_sql ]
        sql = [ LIKE, parent_sql, arg_sql ]
        return BoolExprMonad(monad.translator, sql)
    def strip(monad, chars, strip_type):
        parent_sql = monad.parent.getsql()[0]
        if chars is not None and chars.type is not unicode:
            raise TypeError("'chars' argument must be a string")
        if chars is None:
            return ExprMonad(monad.translator, unicode, [ strip_type, parent_sql ])
        else:
            chars_sql = chars.getsql()[0]
            return ExprMonad(monad.translator, unicode, [ strip_type, parent_sql, chars_sql ])
    def call_strip(monad, chars=None):
        return monad.strip(chars, TRIM)
    def call_lstrip(monad, chars=None):
        return monad.strip(chars, LTRIM)
    def call_rstrip(monad, chars=None):
        return monad.strip(chars, RTRIM)
    
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
    def __new__(cls, translator, type, name, parent=None):
        assert cls is ParamMonad
        type = normalize_type(type)
        if type is int: cls = NumericParamMonad
        elif type is unicode: cls = StringParamMonad
        elif isinstance(type, orm.EntityMeta): cls = ObjectParamMonad
        else: assert False
        return object.__new__(cls)
    def __init__(monad, translator, type, name, parent=None):
        type = normalize_type(type)
        Monad.__init__(monad, translator, type)
        monad.name = name
        monad.parent = parent
        if parent is None: monad.extractor = lambda variables : variables[name]
        else: monad.extractor = lambda variables : getattr(parent.extractor(variables), name)
    def getsql(monad):
        monad.add_extractors()
        return [ [ PARAM, monad.name ] ]
    def add_extractors(monad):
        name = monad.name
        extractors = monad.translator.extractors
        extractors[name] = monad.extractor

class ObjectParamMonad(ObjectMixin, ParamMonad):
    def __init__(monad, translator, entity, name, parent=None):
        if translator.diagram is not entity._diagram_: raise TranslationError(
            'All entities in a query must belong to the same diagram')
        monad.params = [ '-'.join((name, path)) for path in entity._pk_paths_ ]
        ParamMonad.__init__(monad, translator, entity, name, parent)
    def getattr(monad, name):
        entity = monad.type
        attr = entity._adict_[name]
        return ParamMonad(monad.translator, attr.py_type, name, monad)
    def getsql(monad):
        monad.add_extractors()
        return [ [ PARAM, param ] for param in monad.params ]
    def add_extractors(monad):
        entity = monad.type
        extractors = monad.translator.extractors
        if not entity._raw_pk_is_composite_:
            extractors[monad.params[0]] = lambda variables, extractor=monad.extractor : extractor(variables)._raw_pkval_
        else:
            for i, param in enumerate(monad.params):
                extractors[param] = lambda variables, i=i, extractor=monad.extractor : extractor(variables)._raw_pkval_[i]

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
    def __new__(cls, translator, value):
        assert cls is ConstMonad
        value_type = normalize_type(type(value))
        if value_type is int: cls = NumericConstMonad
        elif value_type is unicode: cls = StringConstMonad
        elif value_type is NoneType: cls = NoneMonad
        else: raise TypeError
        return object.__new__(cls)
    def __init__(monad, translator, value):
        value_type = normalize_type(type(value))
        Monad.__init__(monad, translator, value_type)
        monad.value = value
    def getsql(monad):
        return [ [ VALUE, monad.value ] ]

class NoneMonad(Monad):
    def __init__(monad, translator, value=None):
        assert value is None
        ConstMonad.__init__(monad, translator, value)

class StringConstMonad(StringMixin, ConstMonad):
    def len(monad):
        return ExprMonad(monad.translator, int, [ VALUE, len(monad.value) ])
    
class NumericConstMonad(NumericMixin, ConstMonad): pass

class BoolMonad(Monad):
    def __init__(monad, translator):
        monad.translator = translator
        monad.type = bool

class BoolExprMonad(BoolMonad):
    def __init__(monad, translator, sql):
        monad.translator = translator
        monad.type = bool
        monad.sql = sql
    def getsql(monad):
        return monad.sql

cmpops = { '>=' : GE, '>' : GT, '<=' : LE, '<' : LT }        

class CmpMonad(BoolMonad):
    def __init__(monad, op, left, right):
        if not are_comparable_types(op, left.type, right.type): raise TypeError, [left.type, right.type]
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
        assert False

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

class FuncMonad(Monad):
    type = None
    def __init__(monad, translator):
        monad.translator = translator

class FuncLenMonad(FuncMonad):
    type = int
    def __call__(monad, x):
        assert isinstance(x, Monad)
        return x.len()

special_functions = {
    len : FuncLenMonad
}