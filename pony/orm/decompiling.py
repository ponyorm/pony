from __future__ import absolute_import, print_function, division
from pony.py23compat import PY37, PYPY, PY310

import sys, types, inspect
from opcode import opname as opnames, HAVE_ARGUMENT, EXTENDED_ARG, cmp_op
from opcode import hasconst, hasname, hasjrel, haslocal, hascompare, hasfree
from collections import defaultdict

#from pony.thirdparty.compiler import ast, parse
import ast

from pony.utils import throw, get_codeobject_id

##ast.And.__repr__ = lambda self: "And(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)
##ast.Or.__repr__ = lambda self: "Or(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)

class DecompileError(NotImplementedError):
    pass

ast_cache = {}

def decompile(x):
    cells = {}
    t = type(x)
    if t is types.CodeType:
        codeobject = x
    elif t is types.GeneratorType:
        codeobject = x.gi_frame.f_code
    elif t is types.FunctionType:
        codeobject = x.__code__
        if x.__closure__:
            cells = dict(zip(codeobject.co_freevars, x.__closure__))
    else:
        throw(TypeError)
    key = get_codeobject_id(codeobject)
    result = ast_cache.get(key)
    if result is None:
        decompiler = Decompiler(codeobject)
        result = ast.Expr(decompiler.ast), decompiler.external_names
        ast_cache[key] = result
    return result + (cells,)


def simplify(clause):
    if isinstance(clause, ast.BoolOp) and isinstance(clause.op, ast.And):
        if len(clause.values) == 1:
            result = clause.values[0]
        else:
            return clause
    elif isinstance(clause, ast.BoolOp) and isinstance(clause.op, ast.Or):
        if len(clause.values) == 1:
            result = ast.UnaryOp(op=ast.Not(), operand=clause.values[0])
        else:
            return clause
    else:
        return clause
    if getattr(result, 'endpos', 0) < clause.endpos:
        result.endpos = clause.endpos
    return result


class InvalidQuery(Exception):
    pass


def binop(node_type):
    def method(decompiler):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        return ast.BinOp(left=oper1, op=node_type(), right=oper2)
    return method


operator_mapping = {
    '==': ast.Eq,
    '!=': ast.NotEq,
    '<': ast.Lt,
    '<=': ast.LtE,
    '>': ast.Gt,
    '>=': ast.GtE,
    'is': ast.Is,
    'is not': ast.IsNot,
    'in': ast.In,
    'not in': ast.NotIn
}


def clean_assign(node):
    if isinstance(node, ast.Assign):
        return node.targets
    return node


class Decompiler(object):
    def __init__(decompiler, code, start=0, end=None):
        decompiler.code = code
        decompiler.start = decompiler.pos = start
        if end is None: end = len(code.co_code)
        decompiler.end = end
        decompiler.stack = []
        decompiler.jump_map = defaultdict(list)
        decompiler.targets = {}
        decompiler.ast = None
        decompiler.names = set()
        decompiler.assnames = set()
        decompiler.conditions_end = 0
        decompiler.instructions = []
        decompiler.instructions_map = {}
        decompiler.or_jumps = set()
        decompiler.get_instructions()
        decompiler.analyze_jumps()
        decompiler.decompile()
        decompiler.ast = decompiler.stack.pop()
        decompiler.external_names = decompiler.names - decompiler.assnames
        if decompiler.stack:
            throw(DecompileError, 'Compiled code should represent a single expression')
    def get_instructions(decompiler):
        PY36 = sys.version_info >= (3, 6)
        before_yield = True
        code = decompiler.code
        co_code = code.co_code
        free = code.co_cellvars + code.co_freevars
        decompiler.abs_jump_to_top = decompiler.for_iter_pos = -1
        while decompiler.pos < decompiler.end:
            i = decompiler.pos
            op = code.co_code[i]
            if PY36:
                extended_arg = 0
                oparg = code.co_code[i+1]
                while op == EXTENDED_ARG:
                    extended_arg = (extended_arg | oparg) << 8
                    i += 2
                    op = code.co_code[i]
                    oparg = code.co_code[i+1]
                oparg = None if op < HAVE_ARGUMENT else oparg | extended_arg
                i += 2
            else:
                i += 1
                if op >= HAVE_ARGUMENT:
                    oparg = co_code[i] + co_code[i + 1] * 256
                    i += 2
                    if op == EXTENDED_ARG:
                        op = code.co_code[i]
                        i += 1
                        oparg = co_code[i] + co_code[i + 1] * 256 + oparg * 65536
                        i += 2
            if op >= HAVE_ARGUMENT:
                if op in hasconst: arg = [code.co_consts[oparg]]
                elif op in hasname: arg = [code.co_names[oparg]]
                elif op in hasjrel: arg = [i + oparg]
                elif op in haslocal: arg = [code.co_varnames[oparg]]
                elif op in hascompare: arg = [cmp_op[oparg]]
                elif op in hasfree: arg = [free[oparg]]
                else: arg = [oparg]
            else: arg = []
            opname = opnames[op].replace('+', '_')
            if opname == 'FOR_ITER':
                decompiler.for_iter_pos = decompiler.pos
            if opname == 'JUMP_ABSOLUTE' and arg[0] == decompiler.for_iter_pos:
                decompiler.abs_jump_to_top = decompiler.pos

            if before_yield:
                if 'JUMP' in opname:
                    endpos = arg[0]
                    if PY310:
                        endpos *= 2
                    if endpos < decompiler.pos:
                        decompiler.conditions_end = i
                    decompiler.jump_map[endpos].append(decompiler.pos)
                decompiler.instructions_map[decompiler.pos] = len(decompiler.instructions)
                decompiler.instructions.append((decompiler.pos, i, opname, arg))
            if opname == 'YIELD_VALUE':
                before_yield = False
            decompiler.pos = i
    def analyze_jumps(decompiler):
        if PYPY:
            targets = decompiler.jump_map.pop(decompiler.abs_jump_to_top, [])
            decompiler.jump_map[decompiler.for_iter_pos] = targets
            for i, (x, y, opname, arg) in enumerate(decompiler.instructions):
                if 'JUMP' in opname:
                    target = arg[0]
                    if target == decompiler.abs_jump_to_top:
                        decompiler.instructions[i] = (x, y, opname, [decompiler.for_iter_pos])
                        decompiler.conditions_end = y

        i = decompiler.instructions_map[decompiler.conditions_end]
        while i > 0:
            pos, next_pos, opname, arg = decompiler.instructions[i]
            if pos in decompiler.jump_map:
                for jump_start_pos in decompiler.jump_map[pos]:
                    if jump_start_pos > pos:
                        continue
                    for or_jump_start_pos in decompiler.or_jumps:
                        if pos > or_jump_start_pos > jump_start_pos:
                            break  # And jump
                    else:
                        decompiler.or_jumps.add(jump_start_pos)
            i -= 1
    def decompile(decompiler):
        for pos, next_pos, opname, arg in decompiler.instructions:
            if pos in decompiler.targets:
                decompiler.process_target(pos)
            method = getattr(decompiler, opname, None)
            if method is None:
                throw(DecompileError('Unsupported operation: %s' % opname))
            decompiler.pos = pos
            decompiler.next_pos = next_pos
            x = method(*arg)
            if x is not None:
                decompiler.stack.append(x)

    def pop_items(decompiler, size):
        if not size: return ()
        result = decompiler.stack[-size:]
        decompiler.stack[-size:] = []
        return result
    def store(decompiler, node):
        stack = decompiler.stack
        if not stack:
            stack.append(node); return
        top = stack[-1]
        if isinstance(top, ast.Assign):
            target = top.targets
            if isinstance(target, (ast.Tuple, ast.List)) and len(target.elts) < top.count:
                target.elts.append(clean_assign(node))
                if len(target.elts) == top.count:
                    decompiler.store(stack.pop())
            else:
                stack.append(node)
        elif isinstance(top, ast.comprehension):
            assert top.target is None
            if isinstance(node, ast.Assign):
                node = node.targets
            top.target = node
        else:
            stack.append(node)

    BINARY_POWER        = binop(ast.Pow)
    BINARY_MULTIPLY     = binop(ast.Mult)
    BINARY_DIVIDE       = binop(ast.Div)
    BINARY_FLOOR_DIVIDE = binop(ast.FloorDiv)
    BINARY_ADD          = binop(ast.Add)
    BINARY_SUBTRACT     = binop(ast.Sub)
    BINARY_LSHIFT       = binop(ast.LShift)
    BINARY_RSHIFT       = binop(ast.RShift)
    BINARY_AND          = binop(ast.BitAnd)
    BINARY_XOR          = binop(ast.BitXor)
    BINARY_OR           = binop(ast.BitOr)
    BINARY_TRUE_DIVIDE  = BINARY_DIVIDE
    BINARY_MODULO       = binop(ast.Mod)

    def BINARY_SUBSCR(decompiler):
        node2 = decompiler.stack.pop()
        node1 = decompiler.stack.pop()
        if isinstance(node2, ast.Slice):  # and len(node2.nodes) == 2:
            if isinstance(node2.lower, ast.Constant) and node2.lower.value is None:
                node2.lower = None
            if isinstance(node2.upper, ast.Constant) and node2.upper.value is None:
                node2.upper = None
        return ast.Subscript(value=node1, slice=node2, ctx=ast.Load())

    def BUILD_CONST_KEY_MAP(decompiler, length):
        keys = decompiler.stack.pop()
        assert isinstance(keys, ast.Constant)
        keys = [ ast.Constant(key) for key in keys.value ]
        values = decompiler.pop_items(length)
        return ast.Dict(keys=keys, values=values)

    def BUILD_LIST(decompiler, size):
        return ast.List(decompiler.pop_items(size), ctx=ast.Load())

    def BUILD_MAP(decompiler, length):
        if sys.version_info < (3, 5):
            return ast.Dict(())
        data = decompiler.pop_items(2 * length)  # [key1, value1, key2, value2, ...]
        keys, values = [], []
        for i in range(0, len(data), 2):
            keys.append(data[i])
            values.append(data[i+1])
        return ast.Dict(keys=keys, values=values)

    def BUILD_SET(decompiler, size):
        return ast.Set(decompiler.pop_items(size))

    def BUILD_SLICE(decompiler, size):
        return ast.Slice(*decompiler.pop_items(size), ctx=ast.Load())

    def BUILD_TUPLE(decompiler, size):
        return ast.Tuple(decompiler.pop_items(size), ctx=ast.Load())

    def BUILD_STRING(decompiler, count):
        values = list(reversed([decompiler.stack.pop() for _ in range(count)]))
        return ast.JoinedStr(values)

    def CALL_FUNCTION(decompiler, argc, star=None, star2=None):
        pop = decompiler.stack.pop
        kwarg, posarg = divmod(argc, 256)
        keywords = []
        for i in range(kwarg):
            arg = pop()
            key = pop().value
            keywords.append(ast.keyword(key, arg))
        keywords.reverse()
        args = []
        for i in range(posarg):
            args.append(pop())
        args.reverse()
        if star:
            args.append(ast.Starred(value=star))
        if star2:
            keywords.append(ast.keyword(value=star2))
        return decompiler._call_function(args, keywords)

    def _call_function(decompiler, args, keywords=None):
        tos = decompiler.stack.pop()
        if isinstance(tos, ast.GeneratorExp):
            assert len(args) == 1 and not keywords
            genexpr = tos
            qual = genexpr.generators[0]
            assert isinstance(qual.iter, ast.Name)
            assert qual.iter.id == '.0'
            qual.iter = args[0]
            return genexpr
        return ast.Call(tos, args=args, keywords=keywords)

    def CALL_FUNCTION_VAR(decompiler, argc):
        return decompiler.CALL_FUNCTION(argc, decompiler.stack.pop())

    def CALL_FUNCTION_KW(decompiler, argc):
        if sys.version_info < (3, 6):
            return decompiler.CALL_FUNCTION(argc, star2=decompiler.stack.pop())
        keys = decompiler.stack.pop()
        assert isinstance(keys, ast.Constant)
        keys = keys.value
        values = decompiler.pop_items(argc)
        assert len(keys) <= len(values)
        args = values[:-len(keys)]
        keywords = [ast.keyword(k, v) for k, v in zip(keys, values[-len(keys):])]
        return decompiler._call_function(args, keywords)

    def CALL_FUNCTION_VAR_KW(decompiler, argc):
        star2 = decompiler.stack.pop()
        star = decompiler.stack.pop()
        return decompiler.CALL_FUNCTION(argc, star, star2)

    def CALL_FUNCTION_EX(decompiler, argc):
        star2 = None
        if argc:
            if argc != 1:
                throw(DecompileError)
            star2 = decompiler.stack.pop()
        star = decompiler.stack.pop()
        args = [ast.Starred(value=star)] if star else None
        keywords = [ast.keyword(value=star2)] if star2 else None
        return decompiler._call_function(args, keywords)

    def CALL_METHOD(decompiler, argc):
        pop = decompiler.stack.pop
        args = []
        keywords = []
        if argc >= 256:
            kwargc = argc // 256
            argc = argc % 256
            for i in range(kwargc):
                v = pop()
                k = pop()
                assert isinstance(k, ast.Constant)
                k = k.value  # ast.Name(k.value)
                keywords.append(ast.keyword(k, v))
        for i in range(argc):
            args.append(pop())
        args.reverse()
        method = pop()
        return ast.Call(method, args, keywords)

    def COMPARE_OP(decompiler, op):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        op = operator_mapping[op]()
        return ast.Compare(oper1, [op], [oper2])

    def CONTAINS_OP(decompiler, invert):
        return decompiler.COMPARE_OP('not in' if invert else 'in')

    def DUP_TOP(decompiler):
        return decompiler.stack[-1]

    def FOR_ITER(decompiler, endpos):
        target = None
        iter = decompiler.stack.pop()
        ifs = []
        return ast.comprehension(target, iter, ifs, is_async=0)

    def FORMAT_VALUE(decompiler, flags):
        if flags in (0, 1, 2, 3):
            value = decompiler.stack.pop()
            return ast.Str(value, flags)
        elif flags == 4:
            fmt_spec = decompiler.stack.pop()
            value = decompiler.stack.pop()
            return ast.FormattedValue(value, fmt_spec)

    def GEN_START(decompiler, kind):
        assert kind == 0  # only support sync

    def GET_ITER(decompiler):
        pass

    def JUMP_IF_FALSE(decompiler, endpos):
        return decompiler.conditional_jump(endpos, False)

    JUMP_IF_FALSE_OR_POP = JUMP_IF_FALSE

    def JUMP_IF_NOT_EXC_MATCH(decompiler, endpos):
        raise NotImplementedError

    def JUMP_IF_TRUE(decompiler, endpos):
        return decompiler.conditional_jump(endpos, True)

    JUMP_IF_TRUE_OR_POP = JUMP_IF_TRUE

    def conditional_jump(decompiler, endpos, if_true):
        if PY37 or PYPY:
            return decompiler.conditional_jump_new(endpos, if_true)
        return decompiler.conditional_jump_old(endpos, if_true)

    def conditional_jump_old(decompiler, endpos, if_true):
        i = decompiler.next_pos
        if i in decompiler.targets:
            decompiler.process_target(i)
        expr = decompiler.stack.pop()
        clausetype = ast.Or if if_true else ast.And
        clause = ast.BoolOp(op=clausetype(), values=[expr])
        clause.endpos = endpos
        decompiler.targets.setdefault(endpos, clause)
        return clause

    def conditional_jump_new(decompiler, endpos, if_true):
        if PY310:
            endpos *= 2
        expr = decompiler.stack.pop()
        if decompiler.pos >= decompiler.conditions_end:
            clausetype = ast.Or if if_true else ast.And
        elif decompiler.pos in decompiler.or_jumps:
            clausetype = ast.Or
            if not if_true:
                expr = ast.UnaryOp(op=ast.Not(), operand=expr)
        else:
            clausetype = ast.And
            if if_true:
                expr = ast.UnaryOp(op=ast.Not(), operand=expr)
        decompiler.stack.append(expr)

        if decompiler.next_pos in decompiler.targets:
            decompiler.process_target(decompiler.next_pos)

        expr = decompiler.stack.pop()
        clause = ast.BoolOp(op=clausetype(), values=[expr])
        clause.endpos = endpos
        decompiler.targets.setdefault(endpos, clause)
        return clause

    def process_target(decompiler, pos, partial=False):
        if pos is None:
            limit = None
        elif partial:
            limit = decompiler.targets.get(pos, None)
        else:
            limit = decompiler.targets.pop(pos, None)
        top = decompiler.stack.pop()
        while True:
            top = simplify(top)
            if top is limit:
                break
            if isinstance(top, ast.comprehension):
                break
            if not decompiler.stack:
                break
            top2 = decompiler.stack[-1]
            if isinstance(top2, ast.comprehension):
                break
            if partial and hasattr(top2, 'endpos') and top2.endpos == pos:
                break

            if isinstance(top2, ast.BoolOp):
                if isinstance(top, ast.BoolOp) and type(top2.op) is type(top.op):
                    top2.values.extend(top.values)
                else:
                    top2.values.append(top)
            elif isinstance(top2, ast.IfExp):  # Python 2.5
                top2.else_ = top
                if hasattr(top, 'endpos'):
                    top2.endpos = top.endpos
                    if decompiler.targets.get(top.endpos) is top:
                        decompiler.targets[top.endpos] = top2
            else:
                throw(DecompileError('Expression is too complex to decompile, try to pass query as string, '
                                     'e.g. select("x for x in Something")'))
            top2.endpos = max(top2.endpos, getattr(top, 'endpos', 0))
            top = decompiler.stack.pop()
        decompiler.stack.append(top)

    def JUMP_FORWARD(decompiler, endpos):
        i = decompiler.next_pos  # next instruction
        decompiler.process_target(i, True)
        then = decompiler.stack.pop()
        decompiler.process_target(i, False)
        test = decompiler.stack.pop()
        if_exp = ast.IfExp(test=simplify(test), body=simplify(then), orelse=None)
        if_exp.endpos = endpos
        decompiler.targets.setdefault(endpos, if_exp)
        if decompiler.targets.get(endpos) is then:
            decompiler.targets[endpos] = if_exp
        return if_exp

    def IS_OP(decompiler, invert):
        return decompiler.COMPARE_OP('is not' if invert else 'is')

    def LIST_APPEND(decompiler, offset=None):
        throw(InvalidQuery('Use generator expression (... for ... in ...) '
                           'instead of list comprehension [... for ... in ...] inside query'))

    def LIST_EXTEND(decompiler, offset):
        if offset != 1:
            raise NotImplementedError(offset)
        items = decompiler.stack.pop()
        if not isinstance(items, ast.Constant):
            raise NotImplementedError(type(items))
        if not isinstance(items.value, tuple):
            raise NotImplementedError(type(items.value))
        lst = decompiler.stack.pop()
        if not isinstance(lst, ast.List):
            raise NotImplementedError(type(lst))
        values = tuple(ast.Constant(v) for v in items.value)
        lst.elts.extend(values)
        return lst

    def LOAD_ATTR(decompiler, attr_name):
        return ast.Attribute(decompiler.stack.pop(), attr_name, ctx=ast.Load())

    def LOAD_CLOSURE(decompiler, freevar):
        decompiler.names.add(freevar)
        return ast.Name(freevar, ctx=ast.Load())

    def LOAD_CONST(decompiler, const_value):
        return ast.Constant(const_value)

    def LOAD_DEREF(decompiler, freevar):
        decompiler.names.add(freevar)
        return ast.Name(freevar, ctx=ast.Load())

    def LOAD_FAST(decompiler, varname):
        decompiler.names.add(varname)
        return ast.Name(varname, ctx=ast.Load())

    def LOAD_GLOBAL(decompiler, varname):
        decompiler.names.add(varname)
        return ast.Name(varname, ctx=ast.Load())

    def LOAD_METHOD(decompiler, methname):
        return decompiler.LOAD_ATTR(methname)

    LOOKUP_METHOD = LOAD_METHOD  # For PyPy

    def LOAD_NAME(decompiler, varname):
        decompiler.names.add(varname)
        return ast.Name(varname, ctx=ast.Load())

    def MAKE_CLOSURE(decompiler, argc):
        decompiler.stack[-3:-2] = []  # ignore freevars
        return decompiler.MAKE_FUNCTION(argc)

    def MAKE_FUNCTION(decompiler, argc):
        defaults = []
        flags = 0
        if sys.version_info >= (3, 6):
            qualname = decompiler.stack.pop()
            tos = decompiler.stack.pop()
            if argc & 0x08:
                func_closure = decompiler.stack.pop()
            if argc & 0x04:
                annotations = decompiler.stack.pop()
            if argc & 0x02:
                kwonly_defaults = decompiler.stack.pop()
            if argc & 0x01:
                defaults = decompiler.stack.pop()
                throw(DecompileError)
        else:
            qualname = decompiler.stack.pop()
            tos = decompiler.stack.pop()
            if argc:
                defaults = [ decompiler.stack.pop() for i in range(argc) ]
                defaults.reverse()
        codeobject = tos.value
        func_decompiler = Decompiler(codeobject)
        # decompiler.names.update(decompiler.names)  ???
        if codeobject.co_varnames[:1] == ('.0',):
            return func_decompiler.ast  # generator
        argnames, varargs, keywords = inspect.getargs(codeobject)
        if varargs:
            argnames.append(varargs)
            flags |= inspect.CO_VARARGS
        if keywords:
            argnames.append(keywords)
            flags |= inspect.CO_VARKEYWORDS
        return ast.Lambda(argnames, defaults, flags, func_decompiler.ast)

    POP_JUMP_IF_FALSE = JUMP_IF_FALSE
    POP_JUMP_IF_TRUE = JUMP_IF_TRUE

    def POP_TOP(decompiler):
        pass

    def RETURN_VALUE(decompiler):
        if decompiler.next_pos != decompiler.end:
            throw(DecompileError)
        expr = decompiler.stack.pop()
        return simplify(expr)

    def ROT_TWO(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        decompiler.stack.append(tos)
        decompiler.stack.append(tos1)

    def ROT_THREE(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack.pop()
        decompiler.stack.append(tos)
        decompiler.stack.append(tos2)
        decompiler.stack.append(tos1)

    def SETUP_LOOP(decompiler, endpos):
        pass

    def STORE_ATTR(decompiler, attrname):
        decompiler.store(ast.Assign(ast.Attribute(decompiler.stack.pop(), attrname, ctx=ast.Store())))

    def STORE_DEREF(decompiler, freevar):
        decompiler.assnames.add(freevar)
        decompiler.store(ast.Assign(ast.Name(freevar, ctx=ast.Store())))

    def STORE_FAST(decompiler, varname):
        if varname.startswith('_['):
            throw(InvalidQuery('Use generator expression (... for ... in ...) '
                               'instead of list comprehension [... for ... in ...] inside query'))
        decompiler.assnames.add(varname)
        decompiler.store(ast.Assign(ast.Name(varname, ctx=ast.Store())))

    def STORE_MAP(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack[-1]
        if not isinstance(tos2, ast.Dict):
            assert False  # pragma: no cover
        if tos2.items == ():
            tos2.items = []
        tos2.items.append((tos, tos1))

    def STORE_SUBSCR(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack.pop()
        if not isinstance(tos1, ast.Dict):
            assert False  # pragma: no cover
        if tos1.items == ():
            tos1.items = []
        tos1.items.append((tos, tos2))

    def UNARY_POSITIVE(decompiler):
        return ast.UnaryOp(op=ast.UAdd(), operand=decompiler.stack.pop())

    def UNARY_NEGATIVE(decompiler):
        return ast.UnaryOp(op=ast.USub(), operand=decompiler.stack.pop())

    def UNARY_NOT(decompiler):
        return ast.UnaryOp(op=ast.Not(), operand=decompiler.stack.pop())

    def UNARY_INVERT(decompiler):
        return ast.Invert(decompiler.stack.pop())

    def UNPACK_SEQUENCE(decompiler, count):
        ass_tuple = ast.Assign(targets=ast.Tuple([], ctx=ast.Store()))
        ass_tuple.count = count
        return ass_tuple

    def YIELD_VALUE(decompiler):
        expr = decompiler.stack.pop()
        generators = []
        while decompiler.stack:
            decompiler.process_target(None)
            top = decompiler.stack.pop()
            if not isinstance(top, ast.comprehension):
                cond = top
                top = decompiler.stack.pop()
                assert isinstance(top, ast.comprehension)
                top.ifs.append(cond)
                generators.append(top)
            else:
                generators.append(top)
        generators.reverse()
        return ast.GeneratorExp(simplify(expr), generators)

test_lines = """
    (a and b if c and d else e and f for i in T if (A and B if C and D else E and F))

    (a for b in T)
    (a for b, c in T)
    (a for b in T1 for c in T2)
    (a for b in T1 for c in T2 for d in T3)
    (a for b in T if f)
    (a for b in T if f and h)
    (a for b in T if f and h or t)
    (a for b in T if f == 5 and r or t)
    (a for b in T if f and r and t)

    # (a for b in T if f == 5 and +r or not t)
    # (a for b in T if -t and ~r or `f`)

    (a for b in T if x and not y and z)
    (a for b in T if not x and y)
    (a for b in T if not x and y and z)
    (a for b in T if not x and y or z) #FIXME!

    (a**2 for b in T if t * r > y / 3)
    (a + 2 for b in T if t + r > y // 3)
    (a[2,v] for b in T if t - r > y[3])
    ((a + 2) * 3 for b in T if t[r, e] > y[3, r * 4, t])
    (a<<2 for b in T if t>>e > r & (y & u))
    (a|b for c in T1 if t^e > r | (y & (u & (w % z))))

    ([a, b, c] for d in T)
    ([a, b, 4] for d in T if a[4, b] > b[1,v,3])
    ((a, b, c) for d in T)
    ({} for d in T)
    ({'a' : x, 'b' : y} for a, b in T)
    (({'a' : x, 'b' : y}, {'c' : x1, 'd' : 1}) for a, b, c, d in T)
    ([{'a' : x, 'b' : y}, {'c' : x1, 'd' : 1}] for a, b, c, d in T)

    (a[1:2] for b in T)
    (a[:2] for b in T)
    (a[2:] for b in T)
    (a[:] for b in T)
    (a[1:2:3] for b in T)
    (a[1:2, 3:4] for b in T)
    (a[2:4:6,6:8] for a, y in T)

    (a.b.c for d.e.f.g in T)
    # (a.b.c for d[g] in T)

    ((s,d,w) for t in T if (4 != x.a or a*3 > 20) and a * 2 < 5)
    ([s,d,w] for t in T if (4 != x.amount or amount * 3 > 20 or amount * 2 < 5) and amount*8 == 20)
    ([s,d,w] for t in T if (4 != x.a or a*3 > 20 or a*2 < 5 or 4 == 5) and a * 8 == 20)
    (s for s in T if s.a > 20 and (s.x.y == 123 or 'ABC' in s.p.q.r))
    (a for b in T1 if c > d for e in T2 if f < g)

    (func1(a, a.attr, x=123) for s in T)
    # (func1(a, a.attr, *args) for s in T)
    # (func1(a, a.attr, x=123, **kwargs) for s in T)
    (func1(a, b, a.attr1, a.b.c, x=123, y='foo') for s in T)
    # (func1(a, b, a.attr1, a.b.c, x=123, y='foo', **kwargs) for s in T)
    # (func(a, a.attr, keyarg=123) for a in T if a.method(x, *args, **kwargs) == 4)

    ((x or y) and (p or q) for a in T if (a or b) and (c or d))
    (x.y for x in T if (a and (b or (c and d))) or X)

    (a for a in T1 if a in (b for b in T2))
    (a for a in T1 if a in (b for b in T2 if b == a))

    (a for a in T1 if a in (b for b in T2))
    (a for a in T1 if a in select(b for b in T2))
    (a for a in T1 if a in (b for b in T2 if b in (c for c in T3 if c == a)))
    (a for a in T1 if a > x and a in (b for b in T1 if b < y) and a < z)
"""
##   should throw InvalidQuery due to using [] inside of a query
##   (a for a in T1 if a in [b for b in T2 if b in [(c, d) for c in T3]])

##    examples of conditional expressions
##    (a if b else c for x in T)
##    (x for x in T if (d if e else f))
##    (a if b else c for x in T if (d if e else f))
##    (a and b or c and d if x and y or p and q else r and n or m and k for i in T)
##    (i for i in T if (a and b or c and d if x and y or p and q else r and n or m and k))
##    (a and b or c and d if x and y or p and q else r and n or m and k for i in T if (A and B or C and D if X and Y or P and Q else R and N or M and K))

def test():
    import sys
    if sys.version[:3] > '2.4': outmost_iterable_name = '.0'
    else: outmost_iterable_name = '[outmost-iterable]'
    import dis
    for i, line in enumerate(test_lines.split('\n')):
        if not line or line.isspace():
            continue
        line = line.strip()
        if line.startswith('#'):
            continue
        code = compile(line, '<?>', 'eval').co_consts[0]
        ast1 = ast.parse(line).body[0]
        ast1.value.generators[0].iter.id = outmost_iterable_name
        ast1 = ast.dump(ast1, indent=2)
        try:
            ast2 = ast.Expr(Decompiler(code).ast)
            ast2 = ast.dump(ast2, indent=2)
        except Exception as e:
            print()
            print(i, line)
            print()
            print(ast1)
            print()
            dis.dis(code)
            raise
        if ast1 != ast2:
            print()
            print(i, line)
            print()
            print(ast1)
            print()
            print(ast2)
            print()
            dis.dis(code)
            break
        else: print('%d OK: %s' % (i, line))
    else: print('Done!')

if __name__ == '__main__':
    test()
