import types
from compiler import ast
from opcode import opname as opnames, HAVE_ARGUMENT, EXTENDED_ARG, cmp_op
from opcode import hasconst, hasname, hasjrel, haslocal, hascompare, hasfree

from pony.utils import throw

##ast.And.__repr__ = lambda self: "And(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)
##ast.Or.__repr__ = lambda self: "Or(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)

ast_cache = {}

codeobjects = {}

def decompile(x):
    t = type(x)
    if t is types.CodeType: codeobject = x
    elif t is types.GeneratorType: codeobject = x.gi_frame.f_code
    elif t is types.FunctionType: codeobject = x.func_code
    else: throw(TypeError)
    key = id(codeobject)
    result = ast_cache.get(key)
    if result is None:
        codeobjects[key] = codeobject
        decompiler = Decompiler(codeobject)
        result = decompiler.ast, decompiler.external_names
        ast_cache[key] = result
    return result

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

class AstGenerated(Exception): pass

def binop(node_type, args_holder=tuple):
    def method(decompiler):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        return node_type(args_holder((oper1, oper2)))
    return method

class Decompiler(object):
    def __init__(decompiler, code, start=0, end=None):
        decompiler.code = code
        decompiler.start = decompiler.pos = start
        if end is None: end = len(code.co_code)
        decompiler.end = end
        decompiler.stack = []
        decompiler.targets = {}
        decompiler.ast = None
        decompiler.names = set()
        decompiler.assnames = set()
        decompiler.decompile()
        decompiler.ast = decompiler.stack.pop()
        decompiler.external_names = set(decompiler.names - decompiler.assnames)
        assert not decompiler.stack, decompiler.stack
    def decompile(decompiler):
        code = decompiler.code
        co_code = code.co_code
        free = code.co_cellvars + code.co_freevars
        try:
            while decompiler.pos < decompiler.end:
                i = decompiler.pos
                if i in decompiler.targets: decompiler.process_target(i)
                op = ord(code.co_code[i])
                i += 1
                if op >= HAVE_ARGUMENT:
                    oparg = ord(co_code[i]) + ord(co_code[i+1])*256
                    i += 2
                    if op == EXTENDED_ARG:
                        op = ord(code.co_code[i])
                        i += 1
                        oparg = ord(co_code[i]) + ord(co_code[i+1])*256 + oparg*65536
                        i += 2
                    if op in hasconst: arg = [code.co_consts[oparg]]
                    elif op in hasname: arg = [code.co_names[oparg]]
                    elif op in hasjrel: arg = [i + oparg]
                    elif op in haslocal: arg = [code.co_varnames[oparg]]
                    elif op in hascompare: arg = [cmp_op[oparg]]
                    elif op in hasfree: arg = [free[oparg]]
                    else: arg = [oparg]
                else: arg = []
                opname = opnames[op].replace('+', '_')
                # print opname, arg, decompiler.stack
                method = getattr(decompiler, opname, None)
                if method is None: throw(NotImplementedError('Unsupported operation: %s' % opname))
                decompiler.pos = i
                x = method(*arg)
                if x is not None: decompiler.stack.append(x)
        except AstGenerated: pass
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
        if isinstance(oper2, ast.Tuple): return ast.Subscript(oper1, 'OP_APPLY', list(oper2.nodes))
        else: return ast.Subscript(oper1, 'OP_APPLY', [ oper2 ])

    def BUILD_LIST(decompiler, size):
        return ast.List(decompiler.pop_items(size))

    def BUILD_MAP(decompiler, not_used):
        # Pushes a new empty dictionary object onto the stack. The argument is ignored and set to zero by the compiler
        return ast.Dict(())

    def BUILD_SET(decompiler, size):
        return ast.Set(decompiler.pop_items(size))

    def BUILD_SLICE(decompiler, size):
        return ast.Sliceobj(decompiler.pop_items(size))

    def BUILD_TUPLE(decompiler, size):
        return ast.Tuple(decompiler.pop_items(size))

    def CALL_FUNCTION(decompiler, argc, star=None, star2=None):
        pop = decompiler.stack.pop
        kwarg, posarg = divmod(argc, 256)
        args = []
        for i in range(kwarg):
            arg = pop()
            key = pop().value
            args.append(ast.Keyword(key, arg))
        for i in range(posarg): args.append(pop())
        args.reverse()
        tos = pop()
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
        return decompiler.CALL_FUNCTION(argc, None, decompiler.stack.pop())

    def CALL_FUNCTION_VAR_KW(decompiler, argc):
        star2 = decompiler.stack.pop()
        star = decompiler.stack.pop()
        return decompiler.CALL_FUNCTION(argc, star, star2)

    def COMPARE_OP(decompiler, op):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        return ast.Compare(oper1, [(op, oper2)])

    def DUP_TOP(decompiler):
        return decompiler.stack[-1]

    def FOR_ITER(decompiler, endpos):
        assign = None
        iter = decompiler.stack.pop()
        ifs = []
        return ast.GenExprFor(assign, iter, ifs)

    def GET_ITER(decompiler):
        pass

    def JUMP_IF_FALSE(decompiler, endpos):
        return decompiler.conditional_jump(endpos, ast.And)

    JUMP_IF_FALSE_OR_POP = JUMP_IF_FALSE

    def JUMP_IF_TRUE(decompiler, endpos):
        return decompiler.conditional_jump(endpos, ast.Or)

    JUMP_IF_TRUE_OR_POP = JUMP_IF_TRUE

    def conditional_jump(decompiler, endpos, clausetype):
        i = decompiler.pos  # next instruction
        if i in decompiler.targets: decompiler.process_target(i)
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
            else: throw(NotImplementedError('Expression is too complex to decompile, try to pass query as string, e.g. select("x for x in Something")'))
            top2.endpos = max(top2.endpos, getattr(top, 'endpos', 0))
            top = decompiler.stack.pop()
        decompiler.stack.append(top)

    def JUMP_FORWARD(decompiler, endpos):
        i = decompiler.pos  # next instruction
        decompiler.process_target(i, True)
        then = decompiler.stack.pop()
        decompiler.process_target(i, False)
        test = decompiler.stack.pop()
        if_exp = ast.IfExp(simplify(test), simplify(then), None)
        if_exp.endpos = endpos
        decompiler.targets.setdefault(endpos, if_exp)
        if decompiler.targets.get(endpos) is then: decompiler.targets[endpos] = if_exp
        return if_exp

    def LIST_APPEND(decompiler):
        throw(NotImplementedError)

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

    def LOAD_NAME(decompiler, varname):
        decompiler.names.add(varname)
        return ast.Name(varname)

    def MAKE_CLOSURE(decompiler, argc):
        decompiler.stack[-2:-1] = [] # ignore freevars
        return decompiler.MAKE_FUNCTION(argc)

    def MAKE_FUNCTION(decompiler, argc):
        if argc: throw(NotImplementedError)
        tos = decompiler.stack.pop()
        codeobject = tos.value
        func_decompiler = Decompiler(codeobject)
        # decompiler.names.update(decompiler.names)  ???
        if codeobject.co_varnames[:1] == ('.0',):
            return func_decompiler.ast  # generator
        argnames = codeobject.co_varnames[:codeobject.co_argcount]
        defaults = []  # todo
        flags = 0  # todo
        return ast.Lambda(argnames, defaults, flags, func_decompiler.ast)

    POP_JUMP_IF_FALSE = JUMP_IF_FALSE
    POP_JUMP_IF_TRUE = JUMP_IF_TRUE

    def POP_TOP(decompiler):
        pass

    def RETURN_VALUE(decompiler):
        if decompiler.pos != decompiler.end: throw(NotImplementedError)
        expr = decompiler.stack.pop()
        decompiler.stack.append(simplify(expr))
        raise AstGenerated

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
            throw(InvalidQuery('Use generator expression (... for ... in ...) instead of list comprehension [... for ... in ...] inside query'))
        decompiler.assnames.add(varname)
        decompiler.store(ast.AssName(varname, 'OP_ASSIGN'))

    def STORE_MAP(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack[-1]
        if not isinstance(tos2, ast.Dict): assert False
        if tos2.items == (): tos2.items = []
        tos2.items.append((tos, tos1))

    def STORE_SUBSCR(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack.pop()
        if not isinstance(tos1, ast.Dict): assert False
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
        decompiler.stack.append(ast.GenExpr(ast.GenExprInner(simplify(expr), fors)))
        raise AstGenerated

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

    (a for b in T if f == 5 and +r or not t)
    (a for b in T if -t and ~r or `f`)

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

    (func1(a, a.attr, keyarg=123) for s in T)
    (func1(a, a.attr, keyarg=123, *e) for s in T)
    (func1(a, b, a.attr1, a.b.c, keyarg1=123, keyarg2='mx', *e, **f) for s in T)
    (func(a, a.attr, keyarg=123) for a in T if a.method(x, *y, **z) == 4)

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
    import dis, compiler
    for line in test_lines.split('\n'):
        if not line or line.isspace(): continue
        line = line.strip()
        if line.startswith('#'): continue
        code = compile(line, '<?>', 'eval').co_consts[0]
        ast1 = compiler.parse(line).node.nodes[0].expr
        ast1.code.quals[0].iter.name = outmost_iterable_name
        try: ast2 = Decompiler(code).ast
        except Exception, e:
            print
            print line
            print
            print ast1
            print
            dis.dis(code)
            raise
        if str(ast1) != str(ast2):
            print
            print line
            print
            print ast1
            print
            print ast2
            print
            dis.dis(code)
            break
        else: print 'OK: %s' % line
    else: print 'Done!'

if __name__ == '__main__': test()
