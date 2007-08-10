import compiler
 
class CodePrinter:
    def __init__(self):
        self.src = ''
        self.coll = ''
 
    def visitAssName(self, node):
        assert node.flags == 'OP_ASSIGN'
        self.coll += node.name

    def visitAssTuple(self, node):
        self.coll = ''
        comma = ''
        result = ''
        for node_ in node.nodes:
            self.visit(node_)
            result = result + comma + self.coll
            comma = ','
            self.coll = ''
        self.coll = result

    def visitName(self,t):
        self.src += t.name
 
    def visitConst(self,t):
        self.src += str(t.value)
 
    #def visitStmt(self, t):
    #    for i in t:
    #        a = pretty_print(i)
    #        self.src += a + "\n"

    #def visitCompare(self, t):
    #    self.visit(t.expr)
        #self.src += t.expr
        #self.src += t.ops
    #    pass

    def visitGenExprInner(self, node):
        for i, for_ in zip(range(len(node.quals)), node.quals):
            expr =  self.visit(for_) #GenExprFor 
        self.visit(node.expr)
    
    def visitGenExprFor(self, node):
        self.coll = ''
        self.visit(node.assign)
        assign_ = self.coll
        self.coll = ''
        self.visit(node.iter)
        from_ = self.coll
        self.coll = ''
        for if_ in node.ifs:
            self.visit(if_)
        ifs_ = self.coll
        select_ =  "SELECT " + assign_ + " FROM " + from_ 
        if ifs_ != '':
            select_ += " WHERE " + ifs_
        self.coll = ''
        print select_
    
    def visitGetattr(self, t):
        self.src = self.src + "." + t.attrname
        pass
 
    def visitList(self, t):
        self.coll = '[]'
 
def print_sql(text):
    node = compiler.parse(text)
    myvisitor = CodePrinter()
    print text + "\t",
    compiler.walk(node, myvisitor)

print_sql('(a for b in [])')
print_sql('(a for b,c,d in [])')
