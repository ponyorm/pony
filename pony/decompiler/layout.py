g = (     a        for     b      in Student if c > d)
#    expr_inner          assign       iter       ifs 
Module(None, Stmt([Discard(
    
    GenExpr(GenExprInner(Name('a'), [GenExprFor(AssName('b', 'OP_ASSIGN'), Name('Student'), [])]))
    #                     expr      quals
    )]))

GenExpr     | code
GenExprInner| expr
              quals
GenExprFor  | asssign
            | iter
            | ifs


--------------------------------------------------------------------------------------------------
compiler.parse('(s for s in Student)')
Module(None, Stmt([Discard(
    GenExpr(GenExprInner(Name('a'), [GenExprFor(AssName('b', 'OP_ASSIGN'), Name('Student'), [])]))
)]))
>>> dis.dis(g.gi_frame.f_code)
  1           0 SETUP_LOOP              18 (to 21)
              3 LOAD_FAST                0 (.0)
        >>    6 FOR_ITER                11 (to 20)
              9 STORE_FAST               1 (b)
             12 LOAD_GLOBAL              0 (a)
             15 YIELD_VALUE         
             16 POP_TOP             
             17 JUMP_ABSOLUTE            6
        >>   20 POP_BLOCK           
        >>   21 LOAD_CONST               0 (None)
             24 RETURN_VALUE
---------------------------------------------------------------------------------------------------
>>> compiler.parse('(a for b in Student if c > d)')
Module(None, Stmt([Discard(
    GenExpr(GenExprInner(Name('a'), [GenExprFor(AssName('b', 'OP_ASSIGN'), Name('Student'),
                                                [GenExprIf(Compare(Name('c'), [('>', Name('d'))]))])])))
                   ]))
---------------------------------------------------------------------------------------------------
>>> compiler.parse('(a.b for c.d in Student)')
Module(None, Stmt([Discard(
    GenExpr(GenExprInner(Getattr(Name('a'), 'b'), [GenExprFor(AssAttr(Name('c'), 'd', 'OP_ASSIGN'), Name('Student'), [])]))
)]))
>>> dis.dis(g.gi_frame.f_code)
  1           0 SETUP_LOOP              24 (to 27)
              3 LOAD_FAST                0 (.0)
        >>    6 FOR_ITER                17 (to 26)
              9 LOAD_GLOBAL              0 (c)
             12 STORE_ATTR               1 (d)
             15 LOAD_GLOBAL              2 (a)
             18 LOAD_ATTR                3 (b)
             21 YIELD_VALUE         
             22 POP_TOP             
             23 JUMP_ABSOLUTE            6
        >>   26 POP_BLOCK           
        >>   27 LOAD_CONST               0 (None)
             30 RETURN_VALUE        
----------------------------------------------------------------------------------------------------
>>> compiler.parse('(a.b.c for d.e.f in Student)')
Module(None, Stmt([Discard(
    GenExpr(GenExprInner(Getattr(Getattr(Name('a'), 'b'), 'c'),
                         [GenExprFor(AssAttr(Getattr(Name('d'), 'e'), 'f', 'OP_ASSIGN'), Name('Student'), [])]))
)]))
>>> g = (a.b.c for d.e.f in Student)
>>> dis.dis(g.gi_frame.f_code)
  1           0 SETUP_LOOP              30 (to 33)
              3 LOAD_FAST                0 (.0)
        >>    6 FOR_ITER                23 (to 32)
              9 LOAD_GLOBAL              0 (d)
             12 LOAD_ATTR                1 (e)
             15 STORE_ATTR               2 (f)
             18 LOAD_GLOBAL              3 (a)
             21 LOAD_ATTR                4 (b)
             24 LOAD_ATTR                5 (c)
             27 YIELD_VALUE         
             28 POP_TOP             
             29 JUMP_ABSOLUTE            6
        >>   32 POP_BLOCK           
        >>   33 LOAD_CONST               0 (None)
             36 RETURN_VALUE        