from opcode import opname
from opcode import cmp_op
from compiler.ast import * 

from student import *

debug = 1    
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
        self.ast_stack = []
        self.labels = {}
        self.last_label = 0
        self.text = []
        self.expr_inner = None
        self.ast_expr_inner = None
        self.assign = []
        self.ast_assign = []
        self.ifs = None
        self.ast_ifs = []
        self.final_expr = None
        self.ast_final_expr = None
        self.it = None
        self.ast_it = None
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
            if len(self.stack) >= 2:
                while len(self.stack) >= 2:
                    tos, oper = self.stack.pop()
                    tos2, oper2 = self.stack.pop()
                    if debug:
                        print "get from stack TOS=", (tos, oper)
                        print "get from stack TOS1=", (tos2, oper2)
                    tos = "(%s) %s (%s)" % (tos2, oper2, tos)
                    #tos = "%s %s %s" % (tos2, oper2, tos)
                    self.stack.append((tos, oper))
                    if debug:
                        print "new TOS=", (tos, oper)
                tos, oper = self.stack[-1]
                tos = "(%s)" % tos
                self.stack[-1]=(tos, oper)  

    def BINARY_POWER(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s ** %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_MULTIPLY(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s * %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_DIVIDE(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s / %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_FLOOR_DIVIDE(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s // %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_ADD(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s + %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_SUBSTRACT(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s - %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_SUBSCR(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s[%s]' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_LSHIFT(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s << %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_RSHIFT(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s >> %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_AND(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s & %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_XOR(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s ^ %s' % (oper1, oper2)
        self.stack.append(expr)

    def BINARY_OR(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s | %s' % (oper1, oper2)
        self.stack.append(expr)

    BINARY_TRUE_DIVIDE = BINARY_DIVIDE

    def BINARY_MODULO(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s % %s' % (oper1, oper2)
        self.stack.append(expr)

    def BUILD_LIST(self, code):
        oparg = code.get_arg()
        t = [str(self.stack.pop()) for i in range(oparg)]
        t.reverse()
        self.stack.append("[%s]" % ", ".join(t))

    def BUILD_MAP(self, code):
        zero = code.get_arg()
        self.stack.append({})

    def BUILD_SLICE(self, code):
        count = code.get_arg()
        slice = [str(self.stack.pop()) for i in range(count)]
        slice.reverse()
        self.stack.append(":".join(slice))
        if debug: print ""

    def BUILD_TUPLE(self, code):
        oparg = code.get_arg()
        t = [str(self.stack.pop()) for i in range(oparg)]
        t.reverse()
        self.stack.append("(%s)" % ", ".join(t))

    def CALL_FUNCTION(self, code):
        currentop = code.get_currentop()
        oparg = code.get_arg()
        kwarg, posarg = divmod(oparg, 256)
        args = []
        if debug:
            print "posarg=%s kwarg=%s" % (posarg, kwarg)
        if currentop in ('CALL_FUNCTION_KW','CALL_FUNCTION_VAR_KW'):
            args.insert(0, "**%s" % self.stack.pop())
        if currentop in ('CALL_FUNCTION_VAR','CALL_FUNCTION_VAR_KW'):
            args.insert(0, "*%s" % self.stack.pop())
        for i in range(kwarg):
            value = self.stack.pop()
            name = self.stack.pop()
            name = name[1:-1]
            args.insert(0, '%s=%s' % (name, value))
        for i in range(posarg):
            args.insert(0, "%s" % self.stack.pop())
        funcname = self.stack.pop()
        args = ", ".join(args)
        funccall = "%s(%s)" % (funcname, args)
        self.stack.append(funccall)
        if debug:
            print "FUNCTION CALL = %s" % funccall

    CALL_FUNCTION_VAR = CALL_FUNCTION
    CALL_FUNCTION_KW = CALL_FUNCTION
    CALL_FUNCTION_VAR_KW = CALL_FUNCTION


    def COMPARE_OP(self, code):
        oparg = code.get_arg()
        op = cmp_op[oparg]
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        #how to check if oper is digit or string?
        expr = "%s %s %s" % (oper1, op, oper2)
        self.stack.append(expr)
        if debug:
            print "--PUSH EXPR=", expr

    def DUP_TOP(self, code):
        self.stack.append(self.stack[-1])
        if debug:
            print ""

    def FOR_ITER(self, code):
        oparg = code.get_arg()
        code.set_stop(oparg + 1)   # include POP_BLOCK
        self.it = self.stack.pop()
        self.ast_it = self.ast_stack.pop()
        print "iter=", self.it
        print "ast_iter=", self.ast_it

    def GET_ITER(self, code):
        pass

    def JUMP_ABSOLUTE(self, code):
        oparg = code.get_arg()

    JUMP_FORWARD = JUMP_ABSOLUTE
    def JUMP_IF_FALSE(self, code):
        oparg = code.get_arg()
        cmp = self.stack[-1]
        expr = (cmp, 'and')
        self.stack[-1] = expr 
        target = oparg + code.get_current_ip() + 3
        gr = self.labels.setdefault(target, 1)
        self.last_label = target

    def JUMP_IF_TRUE(self, code):
        oparg = code.get_arg()
        cmp = self.stack[-1]
        expr = (cmp, 'or')
        self.stack[-1] = expr 
        target = oparg + code.get_current_ip() + 3
        gr = self.labels.setdefault(target, 1)
        self.last_label = target
        
    def LOAD_ATTR(self, code):
        oparg = code.get_arg()
        varname = code.get_name(oparg)
        tos = self.stack.pop()
        tos = '%s.%s' % (tos, varname)
        self.stack.append(tos)

    def LOAD_CONST(self, code):
        oparg = code.get_arg()
        const = str(code.get_const(oparg))
        if not const.isdigit():
            const = "'%s'" % const
        if const == "'None'":
            const = "None"
        self.stack.append(const)

    def LOAD_DEREF(self, code):
        oparg = code.get_arg()
        varname = code.get_deref(oparg)
        self.stack.append(varname)

    def LOAD_FAST(self, code):
        oparg = code.get_arg()
        varname = code.get_varname(oparg)
        self.stack.append(varname)
        self.ast_stack.append(Name(varname))
        
    def LOAD_GLOBAL(self, code):
        oparg = code.get_arg()
        name = code.get_name(oparg)
        self.stack.append(name)
        self.ast_stack.append(Name(name))

    LOAD_NAME = LOAD_GLOBAL

    def MAKE_FUNCTION(self, code):
        oparg = code.get_arg()
        func = self.stack.pop()
        # call function later
        self.stack.append("FUNCTION CALL")
        if debug: print ""

    def NOP(self, code):
        pass
    
    def POP_BLOCK(self, code):
        if debug:
            print ""
        if len(self.stack) > 0:
            ifexpr, op = self.stack.pop()
            self.ifs = ifexpr
            if debug:
                print "if expr = ", ifexpr

    def POP_TOP(self, code):
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
            print "ast_expr_inner =", d.ast_expr_inner
            print "    ast_assign =", d.ast_assign
            print "      ast_iter =", d.ast_it
            print "       ast_ifs =", d.ast_ifs
            
        self.expr_inner = d.expr_inner
        self.ast_expr_inner = d.ast_expr_inner
        assign = ", ".join(d.assign)
        # TODO
        if len(d.ast_assign) == 1:
            ast_assign = d.ast_assign[0]
        else:
            ast_assign = AssTuple(d.ast_assign)
        expr =  "for %s in %s" %(assign, d.it)        
        if d.ifs:
            expr = "%s if %s" % (expr, d.ifs)
        ast_for = GenExprFor(ast_assign, d.ast_it, d.ast_ifs)
        if self.nesting == 0:
            expr = "%s %s" % (self.expr_inner, expr)
            self.ast_final_expr = GenExprInner(self.ast_expr_inner, ast_for)
        self.final_expr = expr        
        if d.final_expr is not None:
            self.final_expr = "%s %s" % (self.final_expr, d.final_expr)
        
        if debug:
            print "nesting=", self.nesting
            print "for expr =", self.final_expr
            print "labels =", self.labels
            print "self.expr=", self.final_expr
            print "d.final_expr=", d.final_expr
            print "self.ast_expr=", self.ast_final_expr
        code.set_stop(None)

    def SLICE_0(self, code):
        tos = self.stack.pop()
        tos = "%s[:]" % tos
        self.stack.append(tos)

    def SLICE_1(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos = "%s[%s:]" % (tos1, tos)
        self.stack.append(tos)

    def SLICE_2(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos = "%s[:%s]" % (tos1, tos)
        self.stack.append(tos)

    def SLICE_3(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack.pop()
        tos = "%s[%s:%s]" % (tos2, tos1, tos)
        self.stack.append(tos)

    def STORE_ATTR(self, code):
        oparg = code.get_arg()
        varname = code.get_name(oparg)
        tos = self.stack.pop()
        tos = '%s.%s' % (tos, varname)
        self.assign.append(tos)

    def STORE_FAST(self, code):
        oparg = code.get_arg()
        varname = code.get_varname(oparg)        
        self.assign.append(varname) # for 'varname'
        #node = AssName(varname, 'OP_ASSIGN')
        self.ast_assign.append(AssName(varname, 'OP_ASSIGN'))

    def STORE_SUBSCR(self, code):
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2= self.stack.pop()
        tos1[str(tos)] = str(tos2)
        if debug: print ""

    def UNARY_POSITIVE(self, code):
        self.stack.append("+%s" % self.stack.pop())

    def UNARY_NEGATIVE(self, code):
        self.stack.append("-%s" % self.stack.pop())

    def UNARY_NOT(self, code):
        self.stack.append("not %s" % self.stack.pop())
        
    def UNARY_CONVERT(self, code):
        self.stack.append("`%s`" % self.stack.pop())

    def UNARY_INVERT(self, code):
        self.stack.append("~%s" % self.stack.pop())

    def UNPACK_SEQUENCE(self, code):
        count = code.get_arg()
        # for now do nothing with that, may be we need to push to stack 'count' values
        # for having them stored by STORE_FAST later

    def YIELD_VALUE(self, code):
        self.expr_inner = self.stack.pop()
        self.ast_expr_inner = self.ast_stack.pop()
        if debug:
            print ""
        if self.last_label:
            if debug: 
                print "check_current_ip(%s)" % self.last_label
            self.check_current_ip(self.last_label)

def decompile_tostring(g):
	code = Code(g.gi_frame.f_code)
	d = Decompiler()
	d.decompile(code)
	return d.final_expr

def decompile_to_aststring(g):
	code = Code(g.gi_frame.f_code)
	d = Decompiler()
	d.decompile(code)
	return str(d.ast_final_expr)

def ttest():
    g = (a for b in Student)
    g = (a for b in Student if c > d)
    #g = (a for b, c in Student)
    #g = (a for b in Student for c in Student)
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

    #g = (func(a, a.attr, keyarg=123) for a in Student if a.method(x, *y, **z) is not None)
    #g = (func(a, a.attr, b, b.c.d, keyarg1=123, keyarg2=456) for a in Student if a.method(x, x1, *y, **z) is not None)
    #g = ([a, b, c] for a in [] if a > b)
    #g = (a[:] for i in [])
    #g = (a[b:] for i in [])
    #g = (a[:b] for i in [])
    #g = (a[b:c] for i in [])
    
    #g = (a|b for i in [])
    #here add all binary ops

    #g = (~a for i, j in [])
    #here add all unary ops

    #g = ({'a' : x, 'b' : y} for a, b in [])
    # think what to do with " and '

    #g = ({'a' : x, 'b' : y} for a, b in [])

    #g = (a[2:4,6:8] for a in [])    
    #g = (a[2:4:6,6:8] for a, y in [])
    # a[(2:4:6, 6:8)] for a, y in .0 - what to do with ()

    #g = (a(lambda x,y: x > 0) for a in [])    
    #g = (a(b, lambda x,y: x > 0) for a in [])
    #g = (a(b, lambda x,y: x > 0) for a,b,x,y in [])
    
    code = Code(g.gi_frame.f_code)
    d = Decompiler()
    d.decompile(code)
    print str(d.ast_final_expr)

ttest()