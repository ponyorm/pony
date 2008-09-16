from compiler import ast
from opcode import opname as opnames, HAVE_ARGUMENT, EXTENDED_ARG, cmp_op
from opcode import hasconst, hasname, hasjrel, haslocal, hascompare, hasfree

def binop(node_type, args_holder=tuple):
    def method(self):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        return node_type(args_holder((oper1, oper2)))
    return method

def decompile(code):
    return GeneratorDecompiler(code).ast

class AstGenerated(Exception): pass

class GeneratorDecompiler(object):
    def __init__(self, code, start=0, end=None):
        self.code = code
        self.start = self.pos = start
        if end is None: end = len(code.co_code)
        self.end = end
        stack = [ ast.GenExprInner(None, []) ]
        self.stack = stack
        self.ast = None
        self.decompile()
        self.ast = self.stack.pop()
        # assert not self.stack, self.stack
    def decompile(self):
        code = self.code
        co_code = code.co_code
        free = code.co_cellvars + code.co_freevars
        try:
            while self.pos < self.end:
                i = self.pos
                op = ord(code.co_code[i])
                i += 1
                if op >= HAVE_ARGUMENT:
                    oparg = ord(co_code[i]) + ord(co_code[i+1])*256
                    i += 2
                    if op == EXTENDED_ARG:
                        op = ord(code.co_code[i])
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
        return ast.CallFunc(pop(), args, star, star2)

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
        node = ast.GenExprFor(assign, iter, ifs)
        node.endpos = endpos
        return node

    def GET_ITER(self):
        pass

    def JUMP_IF_FALSE(self, endpos):
        self.conditional_jump(endpos, ast.And)

    def JUMP_IF_TRUE(self, endpos):
        self.conditional_jump(endpos, ast.Or)

    def conditional_jump(self, endpos, clausetype):
        expr = self.stack.pop()
        if not self.stack:
            clause = clausetype([ expr ])
            clause.endpos = endpos
            self.stack.append(clause)
        top = self.stack[-1]
        if isinstance(top, (ast.And, ast.Or)):
            if top.endpos == endpos:
                if top.__class__ == clausetype: top.nodes.append(expr)
                else:
                    clause = clausetype([ expr ])
                    clause.endpos = endpos
                    self.stack.append(clause)
            elif top.endpos > endpos:
                clause = clausetype([ expr ])
                clause.endpos = endpos
                self.stack.append(clause)
            else:
                top.nodes.append(expr)
                self.stack.pop()
                if len(self.stack) >= 2:
                    top2 = self.stack[-1]
                    if top2.__class__ == clausetype and top2.endpos == endpos:
                        top2.nodes.append(top)
                        return
                clause = clausetype([ top ])
                clause.endpos = endpos
                self.stack.append(clause)
        else:
            clause = clausetype([ expr ])
            clause.endpos = endpos
            self.stack.append(clause)

    def LOAD_ATTR(self, attr_name):
        return ast.Getattr(self.stack.pop(), attr_name)

    def LOAD_CLOSURE(self, freevar):
        raise NotImplementedError

    def LOAD_CONST(self, const_value):
        return ast.Const(const_value)

    def LOAD_DEREF(self, freevar):
        raise NotImplementedError

    def LOAD_FAST(self, varname):
        return ast.Name(varname)

    def LOAD_GLOBAL(self, varname):
        return ast.Name(varname)

    def LOAD_NAME(self, varname):
        return ast.Name(varname)

    def POP_TOP(self):
        pass

    def RETURN_VALUE(self):
        pass

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

    def STORE_FAST(self, varname):
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

    def pack(self, endpos=None):
        while len(self.stack) >= 2:
            top = self.stack[-1]
            top2 = self.stack[-2]
            if not isinstance(top2, (ast.And, ast.Or)): break
            if endpos is None or top2.endpos > endpos: break
            top2.nodes.append(top)
            self.stack.pop()
        top = self.stack[-1]
        if isinstance(top, ast.And) and len(top.nodes) == 1:
            self.stack.pop()
            self.stack.append(top.nodes[0])

    def YIELD_VALUE(self):
        self.pack(self.pos)
        expr = self.stack.pop()
        fors = []
        while True:
            self.pack()
            top = self.stack.pop()
            if isinstance(top, ast.GenExprInner): break
            elif not isinstance(top, (ast.GenExprFor)):
                cond = ast.GenExprIf(top)
                top = self.stack.pop()
                assert isinstance(top, ast.GenExprFor)
                top.ifs.append(cond)
                fors.append(top)
            else: fors.append(top)
        fors.reverse()
        inner = top
        assert isinstance(inner, ast.GenExprInner) and inner.expr is None and not inner.quals
        inner.expr = expr
        inner.quals = fors
        self.stack.append(ast.GenExpr(inner))
        raise AstGenerated

test_lines = """
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
"""

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
        try: ast2 = decompile(code)
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
    