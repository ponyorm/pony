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
            on = opname[oc]
            self.cp = self.i
            self.i = self.i + 1
            return on
        else:
            raise StopIteration
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
    def __init__(self):
        self.stack = []
        self.labels = {}
        self.last_label = 0
        self.text = []
        self.expr_inner = None
        self.assign = None
        self.ifs = None
        self.final_expr = None
        
    def decompile(self, code):
        try:
            while True:
                on = code.get_nextop()
                if debug:
                    print code.get_current_ip(), on,
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

    def BINARY_MULTIPLY(self, code):
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        expr = '%s * %s' % (oper1, oper2)
        self.stack.append(expr)

    def BUILD_TUPLE(self, code):
        oparg = code.get_arg()
        t = [str(self.stack.pop()) for i in range(oparg)]
        t.reverse()
        self.stack.append("(%s)" % ", ".join(t))

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

    def FOR_ITER(self, code):
        oparg = code.get_arg()
        code.set_stop(oparg)
        d = Decompiler()
        d.decompile(code)
        code.set_stop(None)
        iter = self.stack[-1]
        self.final_expr =  "%s for %s in %s" %(d.expr_inner, d.assign, iter)        
        if d.ifs:
            self.final_expr = "%s if %s" % (self.final_expr, d.ifs)
        if debug:
            print "final expr=", self.final_expr
            print "labels=", d.labels

    def JUMP_ABSOLUTE(self, code):
        oparg = code.get_arg()

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
        self.stack.append(const)

    def LOAD_DEREF(self, code):
        oparg = code.get_arg()
        varname = code.get_deref(oparg)
        self.stack.append(varname)

    def LOAD_FAST(self, code):
        oparg = code.get_arg()
        varname = code.get_varname(oparg)
        self.stack.append(varname)
        
    def LOAD_GLOBAL(self, code):
        oparg = code.get_arg()
        name = code.get_name(oparg)
        self.stack.append(name)

    def SETUP_LOOP(self, code):
        oparg = code.get_arg()        
        code.set_stop(oparg)
        d = Decompiler()
        d.decompile(code)
        self.final_expr = d.final_expr
        code.set_stop(None)

    def STORE_FAST(self, code):
        oparg = code.get_arg()
        varname = code.get_varname(oparg)
        self.assign = varname # for 'varname'
        node = AssName(varname, 'OP_ASSIGN')
        
    def POP_BLOCK(self, code):
        if debug:
            print ""

    def POP_TOP(self, code):
        if debug:
            print ""

    def RETURN_VALUE(self, code):
        if debug:
            print ""

    def YIELD_VALUE(self, code):
        self.expr_inner = self.stack.pop()        
        if debug:
            print ""
        if self.last_label:
            if debug: 
                print "check_current_ip(%s)" % self.last_label
            self.check_current_ip(self.last_label)
        if len(self.stack) > 0:
            expr, op = self.stack.pop()
            self.ifs = expr
            if debug:
                print "--->", expr

def decompile_tostring(g):
	code = Code(g.gi_frame.f_code)
	d = Decompiler()
	d.decompile(code)
	return d.final_expr	

def ttest():
    #g = (s for s in Student)
    #g = (s for s in Student if s.age > 20 and (s.group.number == 4142 or 'FFF' in s.marks.subject.name))
    #g = ( (s,d,w) for t in Student if ((4 != x.a) or (a * 3 > 20) or (a * 2 < 5) and (a * 8 == 20)))
    #g = ( (s,d,w) for t in Student   if ( 4 != x.a  or  a * 3 > 20 ) and ( a * 2 < 5  or  a * 8 == 20 ))
    #g = ( (s,d,w) for t in Student if (((4 != x.a) or (a * 3 > 20)) and (a * 2 < 5) ))
    #g = ( (s,d,w) for t in Student if ((4 != x.a) or (a * 3 > 20) and (a * 2 < 5) ))
    g = ( (s,d,w) for t in Student if ((4 != x.amount or amount * 3 > 20 or amount * 2 < 5) and (amount * 8 == 20)))
    #g = ( (s,t,w) for t in Student if ((4 != x.a.b or a * 3 > 20 or a * 2 < 5 and v == 6) and a * 8 == 20 or (f > 4) ))
    #g = (s for t in Student if a == 5)
    #g = (s for s in Student if a == 5 for f in Student if t > 4 )
    code = Code(g.gi_frame.f_code)
    d = Decompiler()
    d.decompile(code)

ttest()