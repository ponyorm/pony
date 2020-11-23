from __future__ import absolute_import, print_function, division
from pony.py23compat import PY2, izip, xrange, PY37, PYPY

import sys, types, inspect
from opcode import opname as opnames, HAVE_ARGUMENT, EXTENDED_ARG, cmp_op
from opcode import hasconst, hasname, hasjrel, haslocal, hascompare, hasfree
from collections import defaultdict

from pony.thirdparty.compiler import ast, parse

from pony.utils import throw, get_codeobject_id

##ast.And.__repr__ = lambda self: "And(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)
##ast.Or.__repr__ = lambda self: "Or(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)

class DecompileError(NotImplementedError):
    pass

ast_cache = {}

def decompile(x):
    cells = {}
    t = type(x)
    if t is types.CodeType: codeobject = x
    elif t is types.GeneratorType: codeobject = x.gi_frame.f_code
    elif t is types.FunctionType:
        codeobject = x.func_code if PY2 else x.__code__
        if PY2:
            if x.func_closure: cells = dict(izip(codeobject.co_freevars, x.func_closure))
        else:
            if x.__closure__: cells = dict(izip(codeobject.co_freevars, x.__closure__))
    else: throw(TypeError)
    key = get_codeobject_id(codeobject)
    result = ast_cache.get(key)
    if result is None:
        decompiler = Decompiler(codeobject)
        result = decompiler.ast, decompiler.external_names
        ast_cache[key] = result
    return result + (cells,)

def simplify(clause):
    if isinstance(clause, ast.And):
        if len(clause.nodes) == 1: result = clause.nodes[0]
        else: return clause
    elif isinstance(clause, ast.Or):
        if len(clause.nodes) == 1: result = ast.Not(clause.nodes[0])
        else: return clause
    else: return clause
    if getattr(result, 'endpos', 0) < clause.endpos: result.endpos = clause.endpos
    return result

class InvalidQuery(Exception): pass

def binop(node_type, args_holder=tuple):
    def method(decompiler):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        return node_type(args_holder((oper1, oper2)))
    return method

if not PY2: ord = lambda x: x

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
        assert not decompiler.stack, decompiler.stack
    def get_instructions(decompiler):
        PY36 = sys.version_info >= (3, 6)
        before_yield = True
        code = decompiler.code
        co_code = code.co_code
        free = code.co_cellvars + code.co_freevars
        decompiler.abs_jump_to_top = decompiler.for_iter_pos = -1
        while decompiler.pos < decompiler.end:
            i = decompiler.pos
            op = ord(code.co_code[i])
            if PY36:
                extended_arg = 0
                oparg = ord(code.co_code[i+1])
                while op == EXTENDED_ARG:
                    extended_arg = (extended_arg | oparg) << 8
                    i += 2
                    op = ord(code.co_code[i])
                    oparg = ord(code.co_code[i+1])
                oparg = None if op < HAVE_ARGUMENT else oparg | extended_arg
                i += 2
            else:
                i += 1
                if op >= HAVE_ARGUMENT:
                    oparg = ord(co_code[i]) + ord(co_code[i + 1]) * 256
                    i += 2
                    if op == EXTENDED_ARG:
                        op = ord(code.co_code[i])
                        i += 1
                        oparg = ord(co_code[i]) + ord(co_code[i + 1]) * 256 + oparg * 65536
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
        if not stack: stack.append(node); return
        top = stack[-1]
        if isinstance(top, (ast.AssTuple, ast.AssList)) and len(top.nodes) < top.count:
            top.nodes.append(node)
            if len(top.nodes) == top.count: decompiler.store(stack.pop())
        elif isinstance(top, ast.GenExprFor):
            assert top.assign is None
            top.assign = node
        else: stack.append(node)

    BINARY_POWER        = binop(ast.Power)
    BINARY_MULTIPLY     = binop(ast.Mul)
    BINARY_DIVIDE       = binop(ast.Div)
    BINARY_FLOOR_DIVIDE = binop(ast.FloorDiv)
    BINARY_ADD          = binop(ast.Add)
    BINARY_SUBTRACT     = binop(ast.Sub)
    BINARY_LSHIFT       = binop(ast.LeftShift)
    BINARY_RSHIFT       = binop(ast.RightShift)
    BINARY_AND          = binop(ast.Bitand, list)
    BINARY_XOR          = binop(ast.Bitxor, list)
    BINARY_OR           = binop(ast.Bitor, list)
    BINARY_TRUE_DIVIDE  = BINARY_DIVIDE
    BINARY_MODULO       = binop(ast.Mod)

    def BINARY_SUBSCR(decompiler):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        if isinstance(oper2, ast.Sliceobj) and len(oper2.nodes) == 2:
            a, b = oper2.nodes
            a = None if isinstance(a, ast.Const) and a.value == None else a
            b = None if isinstance(b, ast.Const) and b.value == None else b
            return ast.Slice(oper1, 'OP_APPLY', a, b)
        elif isinstance(oper2, ast.Tuple):
            return ast.Subscript(oper1, 'OP_APPLY', list(oper2.nodes))
        else:
            return ast.Subscript(oper1, 'OP_APPLY', [ oper2 ])

    def BUILD_CONST_KEY_MAP(decompiler, length):
        keys = decompiler.stack.pop()
        assert isinstance(keys, ast.Const)
        keys = [ ast.Const(key) for key in keys.value ]
        values = decompiler.pop_items(length)
        pairs = list(izip(keys, values))
        return ast.Dict(pairs)

    def BUILD_LIST(decompiler, size):
        return ast.List(decompiler.pop_items(size))

    def BUILD_MAP(decompiler, length):
        if sys.version_info < (3, 5):
            return ast.Dict(())
        data = decompiler.pop_items(2 * length)  # [key1, value1, key2, value2, ...]
        it = iter(data)
        pairs = list(izip(it, it))  # [(key1, value1), (key2, value2), ...]
        return ast.Dict(tuple(pairs))

    def BUILD_SET(decompiler, size):
        return ast.Set(decompiler.pop_items(size))

    def BUILD_SLICE(decompiler, size):
        return ast.Sliceobj(decompiler.pop_items(size))

    def BUILD_TUPLE(decompiler, size):
        return ast.Tuple(decompiler.pop_items(size))

    def BUILD_STRING(decompiler, count):
        values = list(reversed([decompiler.stack.pop() for _ in range(count)]))
        return ast.JoinedStr(values)

    def CALL_FUNCTION(decompiler, argc, star=None, star2=None):
        pop = decompiler.stack.pop
        kwarg, posarg = divmod(argc, 256)
        args = []
        for i in xrange(kwarg):
            arg = pop()
            key = pop().value
            args.append(ast.Keyword(key, arg))
        for i in xrange(posarg): args.append(pop())
        args.reverse()
        return decompiler._call_function(args, star, star2)

    def _call_function(decompiler, args, star=None, star2=None):
        tos = decompiler.stack.pop()
        if isinstance(tos, ast.GenExpr):
            assert len(args) == 1 and star is None and star2 is None
            genexpr = tos
            qual = genexpr.code.quals[0]
            assert isinstance(qual.iter, ast.Name)
            assert qual.iter.name in ('.0', '[outmost-iterable]')
            qual.iter = args[0]
            return genexpr
        else: return ast.CallFunc(tos, args, star, star2)

    def CALL_FUNCTION_VAR(decompiler, argc):
        return decompiler.CALL_FUNCTION(argc, decompiler.stack.pop())

    def CALL_FUNCTION_KW(decompiler, argc):
        if sys.version_info < (3, 6):
            return decompiler.CALL_FUNCTION(argc, star2=decompiler.stack.pop())
        keys = decompiler.stack.pop()
        assert isinstance(keys, ast.Const)
        keys = keys.value
        values = decompiler.pop_items(argc)
        assert len(keys) <= len(values)
        args = values[:-len(keys)]
        for key, value in izip(keys, values[-len(keys):]):
            args.append(ast.Keyword(key, value))
        return decompiler._call_function(args)

    def CALL_FUNCTION_VAR_KW(decompiler, argc):
        star2 = decompiler.stack.pop()
        star = decompiler.stack.pop()
        return decompiler.CALL_FUNCTION(argc, star, star2)

    def CALL_FUNCTION_EX(decompiler, argc):
        star2 = None
        if argc:
            if argc != 1: throw(DecompileError)
            star2 = decompiler.stack.pop()
        star = decompiler.stack.pop()
        return decompiler._call_function([], star, star2)

    def CALL_METHOD(decompiler, argc):
        pop = decompiler.stack.pop
        args = []
        if argc >= 256:
            kwargc = argc // 256
            argc = argc % 256
            for i in range(kwargc):
                v = pop()
                k = pop()
                assert isinstance(k, ast.Const)
                k = k.value # ast.Name(k.value)
                args.append(ast.Keyword(k, v))
        for i in range(argc):
            args.append(pop())
        args.reverse()
        method = pop()
        return ast.CallFunc(method, args)

    def COMPARE_OP(decompiler, op):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        return ast.Compare(oper1, [(op, oper2)])

    def CONTAINS_OP(decompiler, invert):
        return decompiler.COMPARE_OP('not in' if invert else 'in')

    def DUP_TOP(decompiler):
        return decompiler.stack[-1]

    def FOR_ITER(decompiler, endpos):
        assign = None
        iter = decompiler.stack.pop()
        ifs = []
        return ast.GenExprFor(assign, iter, ifs)

    def FORMAT_VALUE(decompiler, flags):
        if flags in (0, 1, 2, 3):
            value = decompiler.stack.pop()
            return ast.Str(value, flags)
        elif flags == 4:
            fmt_spec = decompiler.stack.pop()
            value = decompiler.stack.pop()
            return ast.FormattedValue(value, fmt_spec)

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
        clause = clausetype([expr])
        clause.endpos = endpos
        decompiler.targets.setdefault(endpos, clause)
        return clause

    def conditional_jump_new(decompiler, endpos, if_true):
        expr = decompiler.stack.pop()
        if decompiler.pos >= decompiler.conditions_end:
            clausetype = ast.Or if if_true else ast.And
        elif decompiler.pos in decompiler.or_jumps:
            clausetype = ast.Or
            if not if_true:
                expr = ast.Not(expr)
        else:
            clausetype = ast.And
            if if_true:
                expr = ast.Not(expr)
        decompiler.stack.append(expr)

        if decompiler.next_pos in decompiler.targets:
            decompiler.process_target(decompiler.next_pos)

        expr = decompiler.stack.pop()
        clause = clausetype([ expr ])
        clause.endpos = endpos
        decompiler.targets.setdefault(endpos, clause)
        return clause

    def process_target(decompiler, pos, partial=False):
        if pos is None: limit = None
        elif partial: limit = decompiler.targets.get(pos, None)
        else: limit = decompiler.targets.pop(pos, None)
        top = decompiler.stack.pop()
        while True:
            top = simplify(top)
            if top is limit: break
            if isinstance(top, ast.GenExprFor): break
            if not decompiler.stack: break
            top2 = decompiler.stack[-1]
            if isinstance(top2, ast.GenExprFor): break
            if partial and hasattr(top2, 'endpos') and top2.endpos == pos: break

            if isinstance(top2, (ast.And, ast.Or)):
                if top2.__class__ == top.__class__: top2.nodes.extend(top.nodes)
                else: top2.nodes.append(top)
            elif isinstance(top2, ast.IfExp):  # Python 2.5
                top2.else_ = top
                if hasattr(top, 'endpos'):
                    top2.endpos = top.endpos
                    if decompiler.targets.get(top.endpos) is top: decompiler.targets[top.endpos] = top2
            else: throw(DecompileError('Expression is too complex to decompile, try to pass query as string, e.g. select("x for x in Something")'))
            top2.endpos = max(top2.endpos, getattr(top, 'endpos', 0))
            top = decompiler.stack.pop()
        decompiler.stack.append(top)

    def JUMP_FORWARD(decompiler, endpos):
        i = decompiler.next_pos  # next instruction
        decompiler.process_target(i, True)
        then = decompiler.stack.pop()
        decompiler.process_target(i, False)
        test = decompiler.stack.pop()
        if_exp = ast.IfExp(simplify(test), simplify(then), None)
        if_exp.endpos = endpos
        decompiler.targets.setdefault(endpos, if_exp)
        if decompiler.targets.get(endpos) is then: decompiler.targets[endpos] = if_exp
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
        if not isinstance(items, ast.Const):
            raise NotImplementedError(type(items))
        if not isinstance(items.value, tuple):
            raise NotImplementedError(type(items.value))
        lst = decompiler.stack.pop()
        if not isinstance(lst, ast.List):
            raise NotImplementedError(type(lst))
        values = tuple(ast.Const(v) for v in items.value)
        lst.nodes = lst.nodes + values
        return lst

    def LOAD_ATTR(decompiler, attr_name):
        return ast.Getattr(decompiler.stack.pop(), attr_name)

    def LOAD_CLOSURE(decompiler, freevar):
        decompiler.names.add(freevar)
        return ast.Name(freevar)

    def LOAD_CONST(decompiler, const_value):
        return ast.Const(const_value)

    def LOAD_DEREF(decompiler, freevar):
        decompiler.names.add(freevar)
        return ast.Name(freevar)

    def LOAD_FAST(decompiler, varname):
        decompiler.names.add(varname)
        return ast.Name(varname)

    def LOAD_GLOBAL(decompiler, varname):
        decompiler.names.add(varname)
        return ast.Name(varname)

    def LOAD_METHOD(decompiler, methname):
        return decompiler.LOAD_ATTR(methname)

    LOOKUP_METHOD = LOAD_METHOD  # For PyPy

    def LOAD_NAME(decompiler, varname):
        decompiler.names.add(varname)
        return ast.Name(varname)

    def MAKE_CLOSURE(decompiler, argc):
        if PY2: decompiler.stack[-2:-1] = [] # ignore freevars
        else: decompiler.stack[-3:-2] = [] # ignore freevars
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
            if not PY2:
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
        if decompiler.next_pos != decompiler.end: throw(DecompileError)
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

    def SLICE_0(decompiler):
        return ast.Slice(decompiler.stack.pop(), 'OP_APPLY', None, None)

    def SLICE_1(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        return ast.Slice(tos1, 'OP_APPLY', tos, None)

    def SLICE_2(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        return ast.Slice(tos1, 'OP_APPLY', None, tos)

    def SLICE_3(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack.pop()
        return ast.Slice(tos2, 'OP_APPLY', tos1, tos)

    def STORE_ATTR(decompiler, attrname):
        decompiler.store(ast.AssAttr(decompiler.stack.pop(), attrname, 'OP_ASSIGN'))

    def STORE_DEREF(decompiler, freevar):
        decompiler.assnames.add(freevar)
        decompiler.store(ast.AssName(freevar, 'OP_ASSIGN'))

    def STORE_FAST(decompiler, varname):
        if varname.startswith('_['):
            throw(InvalidQuery('Use generator expression (... for ... in ...) '
                               'instead of list comprehension [... for ... in ...] inside query'))
        decompiler.assnames.add(varname)
        decompiler.store(ast.AssName(varname, 'OP_ASSIGN'))

    def STORE_MAP(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack[-1]
        if not isinstance(tos2, ast.Dict): assert False  # pragma: no cover
        if tos2.items == (): tos2.items = []
        tos2.items.append((tos, tos1))

    def STORE_SUBSCR(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack.pop()
        if not isinstance(tos1, ast.Dict): assert False  # pragma: no cover
        if tos1.items == (): tos1.items = []
        tos1.items.append((tos, tos2))

    def UNARY_POSITIVE(decompiler):
        return ast.UnaryAdd(decompiler.stack.pop())

    def UNARY_NEGATIVE(decompiler):
        return ast.UnarySub(decompiler.stack.pop())

    def UNARY_NOT(decompiler):
        return ast.Not(decompiler.stack.pop())

    def UNARY_CONVERT(decompiler):
        return ast.Backquote(decompiler.stack.pop())

    def UNARY_INVERT(decompiler):
        return ast.Invert(decompiler.stack.pop())

    def UNPACK_SEQUENCE(decompiler, count):
        ass_tuple = ast.AssTuple([])
        ass_tuple.count = count
        return ass_tuple

    def YIELD_VALUE(decompiler):
        expr = decompiler.stack.pop()
        fors = []
        while decompiler.stack:
            decompiler.process_target(None)
            top = decompiler.stack.pop()
            if not isinstance(top, (ast.GenExprFor)):
                cond = ast.GenExprIf(top)
                top = decompiler.stack.pop()
                assert isinstance(top, ast.GenExprFor)
                top.ifs.append(cond)
                fors.append(top)
            else: fors.append(top)
        fors.reverse()
        return ast.GenExpr(ast.GenExprInner(simplify(expr), fors))

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
    for line in test_lines.split('\n'):
        if not line or line.isspace(): continue
        line = line.strip()
        if line.startswith('#'): continue
        code = compile(line, '<?>', 'eval').co_consts[0]
        ast1 = parse(line).node.nodes[0].expr
        ast1.code.quals[0].iter.name = outmost_iterable_name
        try: ast2 = Decompiler(code).ast
        except Exception as e:
            print()
            print(line)
            print()
            print(ast1)
            print()
            dis.dis(code)
            raise
        if str(ast1) != str(ast2):
            print()
            print(line)
            print()
            print(ast1)
            print()
            print(ast2)
            print()
            dis.dis(code)
            break
        else: print('OK: %s' % line)
    else: print('Done!')

if __name__ == '__main__': test()
