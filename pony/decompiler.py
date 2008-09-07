from opcode import opname
from opcode import cmp_op
from compiler.ast import * 

from student import *

debug = 1

class Expression:
    def __init__(self, operand, operation):
        self.operand = operand
        self.operation = operation
    def __repr__(self):
        return "EXPR(%s %s)" % (self.operand, self.operation)

class Code:
    def __init__(self, code, op_count=None):
        self.code = code.co_code
        self.varnames = code.co_varnames
        self.names = code.co_names
        self.consts = code.co_consts
        self.freevars = code.co_freevars
        self.counter = op_count
        self.i = 0
        self.cp = 0
        self.stop = len(self.code)
        self.indent=0
        self.currentop = None
    def set_stop(self, n):        
        if n:
            if debug:
                print "----- block started"
            self.stop = self.i + n
            self.indent = self.indent + 1
        else:
            self.stop = len(self.code)
            self.indent = self.indent - 1
    def get_nextop(self):
        if self.i < self.stop:
            oc = ord(self.code[self.i])
            self.currentop = opname[oc]
            self.cp = self.i
            self.i = self.i + 1
            return self.currentop
        else:
            raise StopIteration
    def get_currentop(self):
        return self.currentop
    def get_arg(self):
        oparg = ord(self.code[self.i]) + ord(self.code[self.i+1]) * 256
        self.i = self.i + 2
        if debug:
            print "%d" % oparg
        return oparg
    def get_varname(self, n):
        return self.varnames[n]
    def get_name(self, n):
        return self.names[n]
    def get_const(self, n):
        return self.consts[n]
    def get_current_ip(self):
        return self.cp
    def get_deref(self, n):
        return self.freevars[n]

class Decompiler:
    def __init__(self, nesting = None):
        self.stack = []
        self.labels = {}
        self.last_label = 0
        self.text = []
        
        self.expr_inner = None
        self.assign = []
        self.it = None
        self.ifs = []
        
        self.final_expr = None
        
        if nesting is None:
            self.nesting = 0
        else:
            self.nesting = nesting + 1
        
    def decompile(self, code):
        try:
            while True:
                on = code.get_nextop()
                if debug:
                    print code.get_current_ip(), on,
                on = on.replace('+', '_')
                func = getattr(self, on)                
                func(code)
                self.check_current_ip(code.get_current_ip())
        except StopIteration:
            if debug:
                print "----- block finished"

    def check_current_ip(self, ip):
        label = self.labels.get(ip)        
        if label is not None:
            print "stack=", self.stack
            if len(self.stack) >= 2:                
                while len(self.stack) >= 2 and isinstance(self.stack[-1], Expression) and isinstance(self.stack[-2], Expression):
                    tos = self.stack.pop()
                    tos2 = self.stack.pop()
                    if debug:
                        print "get from stack TOS=", tos
                        print "get from stack TOS1=", tos2
                    if tos2.operation == 'And':
                        tos = Expression(And([tos2.operand, tos.operand]), tos.operation)
                    else:
                        tos = Expression(Or([tos2.operand, tos.operand]), tos.operation)
                    #tos = "(%s) %s (%s)" % (tos2, oper2, tos)                    
                    self.stack.append(tos)
                    if debug:
                        print "new TOS=", tos

    def unfold(self, ei):
        if isinstance(ei, dict):
            print "Dict found"
            if len(ei) == 0:
                result = Dict(())
            else:
                result = Dict([i for i in ei.items()])
        else:
            result = ei
        return result

    def BINARY_POWER(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = Power((oper1, oper2))
        self.stack.append(expr)

    def BINARY_MULTIPLY(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = Mul((oper1, oper2))
        self.stack.append(expr)

    def BINARY_DIVIDE(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = Div((oper1, oper2))
        self.stack.append(expr)

    def BINARY_FLOOR_DIVIDE(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = FloorDiv((oper1, oper2))
        self.stack.append(expr)

    def BINARY_ADD(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = Add((oper1, oper2))
        self.stack.append(expr)

    def BINARY_SUBTRACT(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = Sub((oper1, oper2))
        self.stack.append(expr)

    def BINARY_SUBSCR(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        if isinstance(oper2, Tuple):
            oper2 = list(oper2.asList())
        else:
            oper2 = [oper2]
        expr = Subscript(oper1, 'OP_APPLY', oper2)
        self.stack.append(expr)

    def BINARY_LSHIFT(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = LeftShift((oper1, oper2))
        self.stack.append(expr)

    def BINARY_RSHIFT(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = RightShift((oper1, oper2))
        self.stack.append(expr)

    def BINARY_AND(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = Bitand([oper1, oper2])
        self.stack.append(expr)

    def BINARY_XOR(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = Bitxor([oper1, oper2])
        self.stack.append(expr)

    def BINARY_OR(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = Bitor([oper1, oper2])
        self.stack.append(expr)

    BINARY_TRUE_DIVIDE = BINARY_DIVIDE

    def BINARY_MODULO(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = Mod((oper1, oper2))
        self.stack.append(expr)

    def BUILD_LIST(self, code):
        oparg = code.get_arg()
        if oparg == 0:
            self.stack.append(List(()))     # to be consistent to compile.parse([])
        else:
            t = [self.unfold(self.stack.pop()) for i in range(oparg)]
            t.reverse()
            self.stack.append(List(t))

    def BUILD_MAP(self, code):
        zero = code.get_arg()      # Pushes a new empty dictionary object onto the stack. The argument is ignored and set to zero by the compiler
        self.stack.append({})

    def BUILD_SLICE(self, code):
        count = code.get_arg()
        slice = [self.stack.pop() for i in range(count)]
        slice.reverse()
        self.stack.append(Sliceobj(slice)) 
        if debug: print ""

    def BUILD_TUPLE(self, code):
        oparg = code.get_arg()
        t = [self.unfold(self.stack.pop()) for i in range(oparg)]
        t.reverse()
        self.stack.append(Tuple(t))

    def CALL_FUNCTION(self, code):
        currentop = code.get_currentop()
        oparg = code.get_arg()
        kwarg, posarg = divmod(oparg, 256)
        args = []
        var = varkw = None
        if debug:
            print "posarg=%s kwarg=%s" % (posarg, kwarg)
        if currentop in ('CALL_FUNCTION_KW','CALL_FUNCTION_VAR_KW'):
            varkw = self.stack.pop()
        if currentop in ('CALL_FUNCTION_VAR','CALL_FUNCTION_VAR_KW'):
            var = self.stack.pop()
        for i in range(kwarg):
            value = self.stack.pop()
            name = self.stack.pop()
            args.insert(0, Keyword(name.value, value))   # no Const in keyargs
        for i in range(posarg):
            args.insert(0, self.stack.pop())
        funcname = self.stack.pop()
        funccall = CallFunc(funcname, args, var, varkw)
        self.stack.append(funccall)
        if debug:
            print "FUNCTION CALL = %s" % funccall

    CALL_FUNCTION_VAR = CALL_FUNCTION
    CALL_FUNCTION_KW = CALL_FUNCTION
    CALL_FUNCTION_VAR_KW = CALL_FUNCTION


    def COMPARE_OP(self, code):
        print "stack=", self.stack
        oparg = code.get_arg()
        op = cmp_op[oparg]
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        print "oper1=", oper1, " op=", op, " oper2=", oper2        
        expr = Compare(oper1, [(op, oper2)])
        self.stack.append(expr)
        #self.stack.append("FAKE COMPARE")
        if debug:
            print "--PUSH EXPR=", expr

    def DUP_TOP(self, code):
        self.stack.append(self.stack[-1])
        if debug:
            print ""

    def FOR_ITER(self, code):
        code.set_stop(code.get_arg() + 1)   # include POP_BLOCK
        self.it = self.stack[-1]
        self.stack.append('FAKE(FOR_ITER)')
        print "iter=", self.it

    def GET_ITER(self, code):
        pass

    def JUMP_ABSOLUTE(self, code):
        oparg = code.get_arg()

    JUMP_FORWARD = JUMP_ABSOLUTE
    
    def JUMP_IF_FALSE(self, code):
        oparg = code.get_arg()
        cmp = self.stack[-1]
        expr = Expression(cmp, 'And')
        self.stack[-1] = expr 
        target = oparg + code.get_current_ip() + 3
        gr = self.labels.setdefault(target, 1)
        self.last_label = target

    def JUMP_IF_TRUE(self, code):
        oparg = code.get_arg()
        cmp = self.stack[-1]
        expr = Expression(cmp, 'Or')
        self.stack[-1] = expr 
        target = oparg + code.get_current_ip() + 3
        gr = self.labels.setdefault(target, 1)
        self.last_label = target
        
    def LOAD_ATTR(self, code):
        oparg = code.get_arg()
        varname = code.get_name(oparg)
        tos = self.stack.pop()
        self.stack.append(Getattr(tos, varname))

    def LOAD_CONST(self, code):
        oparg = code.get_arg()
        const = Const(code.get_const(oparg))
        self.stack.append(const)

    def LOAD_DEREF(self, code):        
        varname = code.get_deref(code.get_arg())
        self.stack.append(varname)

    def LOAD_FAST(self, code):
        varname = code.get_varname(code.get_arg())        
        self.stack.append(Name(varname))
        
    def LOAD_GLOBAL(self, code):
        name = code.get_name(code.get_arg())
        self.stack.append(Name(name))

    LOAD_NAME = LOAD_GLOBAL

    def MAKE_FUNCTION(self, code):
        oparg = code.get_arg()
        func = self.stack.pop()
        # call function later
        arglist = [self.stack.pop() for i in range(oparg)]
        self.stack.append("FUNCTION CALL: %s (%s)" % (func, arglist))
        if debug: print ""

    def NOP(self, code):
        pass
    
    def POP_BLOCK(self, code):
        if debug:
            print ""
        if len(self.stack) > 0:
            print "stack=", self.stack
            ifexpr = []
            while len(self.stack) > 0 and isinstance(self.stack[-1], Expression):                       
                expr = self.stack.pop()
                ifexpr.append(GenExprIf(expr.operand))
            ifexpr.reverse()
            self.ifs = ifexpr
            if debug:
                print "if expr = ", ifexpr

    def POP_TOP(self, code):
        print "stack=", self.stack
        #if len(self.stack) > 0:
        #    self.stack.pop()
        if debug:
            print ""

    def RETURN_VALUE(self, code):
        if debug:
            print ""

    def ROT_TWO(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        self.stack.append(tos)
        self.stack.append(tos1)
        if debug:
            print ""

    def ROT_THREE(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack.pop()
        self.stack.append(tos)
        self.stack.append(tos2)
        self.stack.append(tos1)
        if debug:
            print ""

    def SETUP_LOOP(self, code):
        oparg = code.get_arg()        
        code.set_stop(oparg + 1) # include POP_BLOCK
        d = Decompiler(self.nesting)
        d.decompile(code)
        if debug:
            print "expr_inner =", d.expr_inner
            print "    assign =", d.assign
            print "      iter =", d.it
            print "       ifs =", d.ifs
            
        self.expr_inner = d.expr_inner
        if len(d.assign) == 1:
            assign = d.assign[0]
        else:
            assign = AssTuple(d.assign)
            
        ast_for = [GenExprFor(assign, d.it, d.ifs)]
        if d.final_expr != None:
            ast_for.extend(d.final_expr)
        if self.nesting == 0: # the most outer loop            
            self.final_expr = GenExprInner(self.expr_inner, ast_for)
        else:
            self.final_expr = ast_for
        
        if debug:
            print "nesting=", self.nesting
            print "labels =", self.labels
            print "self.ast_expr=", self.final_expr
        code.set_stop(None)

    def SLICE_0(self, code):
        tos = self.stack.pop()
        self.stack.append(Slice(tos, 'OP_APPLY', None, None))

    def SLICE_1(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        self.stack.append(Slice(tos1, 'OP_APPLY', tos, None))

    def SLICE_2(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        self.stack.append(Slice(tos1, 'OP_APPLY', None, tos))

    def SLICE_3(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack.pop()
        self.stack.append(Slice(tos2, 'OP_APPLY', tos1, tos))

    def STORE_ATTR(self, code):
        oparg = code.get_arg()
        varname = code.get_name(oparg)
        tos = self.stack.pop()
        self.assign.append(AssAttr(tos, varname, 'OP_ASSIGN'))

    def STORE_FAST(self, code):
        varname = code.get_varname(code.get_arg())        
        self.assign.append(AssName(varname, 'OP_ASSIGN')) # for 'varname'
        self.stack.pop()

    def STORE_SUBSCR(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack.pop()
        if tos2 == 'FAKE(FOR_ITER)':
            self.assign.append(Subscript(tos1, 'OP_ASSIGN', [tos]))
        else:
            tos1[tos] = tos2
        if debug: print ""

    def UNARY_POSITIVE(self, code):
        self.stack.append(UnaryAdd(self.stack.pop()))

    def UNARY_NEGATIVE(self, code):
        self.stack.append(UnarySub(self.stack.pop()))

    def UNARY_NOT(self, code):
        self.stack.append(Not(self.stack.pop()))
        
    def UNARY_CONVERT(self, code):        
        self.stack.append(Backquote(self.stack.pop()))

    def UNARY_INVERT(self, code):
        self.stack.append(Invert(self.stack.pop()))

    def UNPACK_SEQUENCE(self, code):
        count = code.get_arg()
        for i in range(count):
            self.stack.append('UNPACK_SEQUENCE %s' % i)
        # push to stack 'count' values
        # and store them later with STORE_FAST        

    def YIELD_VALUE(self, code):
        ei = self.stack.pop()
        self.expr_inner = self.unfold(ei)
        if debug:
            print ""
        if self.last_label:
            if debug: 
                print "check_current_ip(%s)" % self.last_label
            self.check_current_ip(self.last_label)

def decompile_to_ast(g):
	code = Code(g.gi_frame.f_code)
	d = Decompiler()
	d.decompile(code)
	return d.final_expr

def ttest():
    #g = (a for b in Student)
    #g = (a for b in Student for c in [])
    #g = (a for b in Student for c in [] for d in [])
    g = (a for b, c in Student)
    #g = (a for b in Student if f)
    #g = (a for b in Student if f if r)
    #g = (a for b in Student if f and r)
    #g = (a for b in Student if f and r or t)
    #g = (a for b in Student if f == 5 and r or not t)
    #g = (a for b in Student if not -t)
    #g = (a for b in Student if t is None)
    #g = (a ** 2 for b in Student if t ** r == y)
    #g = (a|b for c in Student if t^e > r | (y & (u & (w % z))))

    #g = ([a, b, c] for a in [] if a > b)
    #g = ([a, b, 4] for d in Student if a[4,5] > b[1,2,3])
    #g = ((a, b, c) for a in [] if a > b)

    #g = (a[2:4,6:8] for a in [])    
    #g = (a[2:4:6,6:8] for a, y in [])
    # a[(2:4:6, 6:8)] for a, y in .0 - what to do with ()

    #g = (a[:] for i in [])
    #g = (a[b:] for i in [])
    #g = (a[:b] for i in [])
    #g = (a[b:c] for i in [])
    
    #g = (a|b for i in [])
    #here add all binary ops

    #g = (~a for i, j in [])
    #here add all unary ops

    #g = ({'a' : x, 'b' : y} for a, b in [])    

    #g = (a.b.c for d.e.f in Student)
    #g = (a for b in Student if c > d)
    #g = (a for b in Student if c > d for e in Student if f < g)
    #g = (s for s in Student if s.age > 20 and (s.group.number == 4142 or 'FFF' in s.marks.subject.name))
    #g = ( (s,d,w) for t in Student if ((4 != x.a) or (a * 3 > 20) or (a * 2 < 5) and (a * 8 == 20)))
    #g = ( (s,d,w) for t in Student   if ( 4 != x.a  or  a * 3 > 20 ) and ( a * 2 < 5  or  a * 8 == 20 ))
    #g = ( (s,d,w) for t in Student if (((4 != x.a) or (a * 3 > 20)) and (a * 2 < 5) ))
    #g = ( (s,d,w) for t in Student if ((4 != x.a) or (a * 3 > 20) and (a * 2 < 5) ))
    #g = ( (s,d,w) for t in Student if ((4 != x.amount or amount * 3 > 20 or amount * 2 < 5) and (amount * 8 == 20)))
    #g = ( (s,t,w) for t in Student if ((4 != x.a.b or a * 3 > 20 or a * 2 < 5 and v == 6) and a * 8 == 20 or (f > 4) ))
    #g = (s for t in Student if a == 5)
    #g = (s for s in Student if a == 5 for f in Student if t > 4 )

    #g = (func1(a, a.attr, keyarg=123, *e) for s in Student)
    #g = (func(a, a.attr, keyarg=123) for a in Student if a.method(x, *y, **z) is not None)
    #g = (func(a, a.attr, b, b.c.d, keyarg1=123, keyarg2=456) for a in Student if a.method(x, x1, *y, **z) is not None)

    #g = (a(lambda x,y: x > 0) for a in [])    
    #g = (a(b, lambda x,y: x > 0) for a in [])
    #g = (a(b, lambda x,y: x > 0) for a,b,x,y in [])

    #g = (a for b in Student if c > d > e)
    #g = (a for b in Student if c > d > d2)

    
    print decompile_to_ast(g)

ttest()