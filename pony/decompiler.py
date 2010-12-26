from compiler import ast
from opcode import opname as opnames, HAVE_ARGUMENT, EXTENDED_ARG, cmp_op
from opcode import hasconst, hasname, hasjrel, haslocal, hascompare, hasfree

##ast.And.__repr__ = lambda self: "And(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)
##ast.Or.__repr__ = lambda self: "Or(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)

def binop(node_type, args_holder=tuple):
    def method(self):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        return node_type(args_holder((oper1, oper2)))
    return method

ast_cache = {}

def decompile(gen):
    codeobject = gen.gi_frame.f_code
    result = ast_cache.get(codeobject)
    if result is None:
        decompiler = GeneratorDecompiler(codeobject)
        result = decompiler.ast.code, decompiler.external_names
        ast_cache[codeobject] = result
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

class AstGenerated(Exception): pass

class GeneratorDecompiler(object):
    def __init__(self, code, start=0, end=None):
        self.code = code
        self.start = self.pos = start
        if end is None: end = len(code.co_code)
        self.end = end
        self.stack = []
        self.targets = {}
        self.ast = None
        self.names = set()
        self.assnames = set()
        self.decompile()
        self.ast = self.stack.pop()
        self.external_names = set(self.names - self.assnames)
        assert not self.stack, self.stack
    def decompile(self):
        code = self.code
        co_code = code.co_code
        free = code.co_cellvars + code.co_freevars
        try:
            while self.pos < self.end:
                i = self.pos
                if i in self.targets: self.process_target(i)
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
                # print opname, arg, self.stack
                method = getattr(self, opname, None)
                if method is None: raise NotImplementedError('Unsupported operation: %s' % opname)
                self.pos = i
                x = method(*arg)
                if x is not None: self.stack.append(x)
        except AstGenerated: pass
    def pop_items(self, size):
        if not size: return ()
        result = self.stack[-size:]
        self.stack[-size:] = []
        return result
    def store(self, node):
        stack = self.stack
        if not stack: stack.append(node); return
        top = stack[-1]
        if isinstance(top, (ast.AssTuple, ast.AssList)) and len(top.nodes) < top.count:
            top.nodes.append(node)
            if len(top.nodes) == top.count: self.store(stack.pop())
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
    
    def BINARY_SUBSCR(self):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        if isinstance(oper2, ast.Tuple): return ast.Subscript(oper1, 'OP_APPLY', list(oper2.nodes))
        else: return ast.Subscript(oper1, 'OP_APPLY', [ oper2 ])

    def BUILD_LIST(self, size):
        return ast.List(self.pop_items(size))

    def BUILD_MAP(self, not_used):
        # Pushes a new empty dictionary object onto the stack. The argument is ignored and set to zero by the compiler
        return ast.Dict(())

    def BUILD_SLICE(self, size):
        return ast.Sliceobj(self.pop_items(size))
        
    def BUILD_TUPLE(self, size):
        return ast.Tuple(self.pop_items(size))

    def CALL_FUNCTION(self, argc, star=None, star2=None):
        pop = self.stack.pop
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

    def CALL_FUNCTION_VAR(self, argc):
        return self.CALL_FUNCTION(argc, self.stack.pop())

    def CALL_FUNCTION_KW(self, argc):
        return self.CALL_FUNCTION(argc, None, self.stack.pop())

    def CALL_FUNCTION_VAR_KW(self, argc):
        star2 = self.stack.pop()
        star = self.stack.pop()
        return self.CALL_FUNCTION(argc, star, star2)

    def COMPARE_OP(self, op):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        return ast.Compare(oper1, [(op, oper2)])

    def DUP_TOP(self):
        return self.stack[-1]

    def FOR_ITER(self, endpos):
        assign = None
        iter = self.stack.pop()
        ifs = []
        return ast.GenExprFor(assign, iter, ifs)

    def GET_ITER(self):
        pass

    def JUMP_IF_FALSE(self, endpos):
        return self.conditional_jump(endpos, ast.And)

    def JUMP_IF_TRUE(self, endpos):
        return self.conditional_jump(endpos, ast.Or)

    def conditional_jump(self, endpos, clausetype):
        i = self.pos  # next instruction
        if i in self.targets: self.process_target(i)
        expr = self.stack.pop()
        clause = clausetype([ expr ])
        clause.endpos = endpos
        self.targets.setdefault(endpos, clause)
        return clause

    def process_target(self, pos, partial=False):
        if pos is None: limit = None
        elif partial: limit = self.targets.get(pos, None)
        else: limit = self.targets.pop(pos, None)
        top = self.stack.pop()
        while True:
            top = simplify(top)
            if top is limit: break
            if isinstance(top, ast.GenExprFor): break

            top2 = self.stack[-1]
            if isinstance(top2, ast.GenExprFor): break
            if partial and hasattr(top2, 'endpos') and top2.endpos == pos: break

            if isinstance(top2, (ast.And, ast.Or)):
                if top2.__class__ == top.__class__: top2.nodes.extend(top.nodes)
                else: top2.nodes.append(top)
            elif isinstance(top2, ast.IfExp):  # Python 2.5
                top2.else_ = top
                if hasattr(top, 'endpos'):
                    top2.endpos = top.endpos
                    if self.targets.get(top.endpos) is top: self.targets[top.endpos] = top2
            else: assert False
            top2.endpos = max(top2.endpos, getattr(top, 'endpos', 0))
            top = self.stack.pop()
        self.stack.append(top)

    def JUMP_FORWARD(self, endpos):
        i = self.pos  # next instruction
        self.process_target(i, True)
        then = self.stack.pop()
        self.process_target(i, False)
        test = self.stack.pop()
        if_exp = ast.IfExp(simplify(test), simplify(then), None)
        if_exp.endpos = endpos
        self.targets.setdefault(endpos, if_exp)
        if self.targets.get(endpos) is then: self.targets[endpos] = if_exp
        return if_exp

    def LIST_APPEND(self):
        raise NotImplementedError

    def LOAD_ATTR(self, attr_name):
        return ast.Getattr(self.stack.pop(), attr_name)

    def LOAD_CLOSURE(self, freevar):
        self.names.add(freevar)
        return ast.Name(freevar)

    def LOAD_CONST(self, const_value):
        return ast.Const(const_value)

    def LOAD_DEREF(self, freevar):
        self.names.add(freevar)
        return ast.Name(freevar)

    def LOAD_FAST(self, varname):
        self.names.add(varname)
        return ast.Name(varname)

    def LOAD_GLOBAL(self, varname):
        self.names.add(varname)
        return ast.Name(varname)

    def LOAD_NAME(self, varname):
        self.names.add(varname)
        return ast.Name(varname)

    def MAKE_CLOSURE(self, argc):
        self.stack[-2:-1] = [] # ignore freevars
        return self.MAKE_FUNCTION(argc)

    def MAKE_FUNCTION(self, argc):
        if argc: raise NotImplementedError
        tos = self.stack.pop()
        codeobject = tos.value
        decompiler = GeneratorDecompiler(codeobject)
        self.names.update(decompiler.names)
        return decompiler.ast

    def POP_TOP(self):
        pass

    def RETURN_VALUE(self):
        raise NotImplementedError

    def ROT_TWO(self):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        self.stack.append(tos)
        self.stack.append(tos1)

    def ROT_THREE(self):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack.pop()
        self.stack.append(tos)
        self.stack.append(tos2)
        self.stack.append(tos1)

    def SETUP_LOOP(self, endpos):
        pass

    def SLICE_0(self):
        return ast.Slice(self.stack.pop(), 'OP_APPLY', None, None)
    
    def SLICE_1(self):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        return ast.Slice(tos1, 'OP_APPLY', tos, None)
    
    def SLICE_2(self):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        return ast.Slice(tos1, 'OP_APPLY', None, tos)
    
    def SLICE_3(self):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack.pop()
        return ast.Slice(tos2, 'OP_APPLY', tos1, tos)
    
    def STORE_ATTR(self, attrname):
        self.store(ast.AssAttr(self.stack.pop(), attrname, 'OP_ASSIGN'))

    def STORE_DEREF(self, freevar):
        self.assnames.add(freevar)
        self.store(ast.AssName(freevar, 'OP_ASSIGN'))

    def STORE_FAST(self, varname):
        self.assnames.add(varname)
        self.store(ast.AssName(varname, 'OP_ASSIGN'))

    def STORE_SUBSCR(self):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack.pop()
        if isinstance(tos2, ast.GenExprFor):
            assert False
            self.assign.append(Subscript(tos1, 'OP_ASSIGN', [tos]))
        elif isinstance(tos1, ast.Dict):
            if tos1.items == (): tos1.items = []
            tos1.items.append((tos, tos2))
        else: assert False

    def UNARY_POSITIVE(self):
        return ast.UnaryAdd(self.stack.pop())

    def UNARY_NEGATIVE(self):
        return ast.UnarySub(self.stack.pop())

    def UNARY_NOT(self):
        return ast.Not(self.stack.pop())
        
    def UNARY_CONVERT(self):
        return ast.Backquote(self.stack.pop())

    def UNARY_INVERT(self):
        return ast.Invert(self.stack.pop())

    def UNPACK_SEQUENCE(self, count):
        ass_tuple = ast.AssTuple([])
        ass_tuple.count = count
        return ass_tuple

    def YIELD_VALUE(self):
        expr = self.stack.pop()
        fors = []
        while self.stack:
            self.process_target(None)
            top = self.stack.pop()
            if not isinstance(top, (ast.GenExprFor)):
                cond = ast.GenExprIf(top)
                top = self.stack.pop()
                assert isinstance(top, ast.GenExprFor)
                top.ifs.append(cond)
                fors.append(top)
            else: fors.append(top)
        fors.reverse()
        self.stack.append(ast.GenExpr(ast.GenExprInner(simplify(expr), fors)))
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
"""

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
        try: ast2 = GeneratorDecompiler(code).ast
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
    