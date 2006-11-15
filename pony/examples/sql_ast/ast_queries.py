from pony.sqlsymbols import *

# param1 = 4142
# param2 = 20
#
# query1 = select(
#     s for s in Group(number=param1).students
#       if s.age > param2
#     )

select1a = [ SELECT,
    [ ALL, [COLUMN, 'T2', 'id'],
           [COLUMN, 'T2', 'first_name'],
           [COLUMN, 'T2', 'last_name'],
           [COLUMN, 'T2', 'group'],
           [COLUMN, 'T2', 'age']
    ],
    [ FROM, [ 'T1', TABLE, 'Groups' ],
            [ 'T2', TABLE, 'Students',
              [ EQ, [COLUMN, 'T1', 'number'], [COLUMN, 'T2', 'group'] ]
            ]
    ],
    [ WHERE, [ AND, [ EQ, [COLUMN, 'T1', 'number'], [PARAM, 1] ],
                    [ LT, [COLUMN, 'T2', 'age'], [PARAM, 2] ]
             ]
    ],
]

select1b = [ SELECT,
    [ ALL, [COLUMN, 'T1', 'id'],
           [COLUMN, 'T1', 'first_name'],
           [COLUMN, 'T1', 'last_name']
    ],
    [ FROM, ['T1', TABLE, 'Students'] ],
    [ WHERE, [ AND, [ EQ, [COLUMN, 'T1', 'group'], [PARAM, 1] ],
                    [ LT, [COLUMN, 'T1', 'age'], [PARAM, 2] ]
             ]
    ],
]

# query2 = select(
#     (s.first_name, s.age) for g in Group
#                           for s in g.students
#                           if g.spec = 2201
# ).order_by(-2)[:10]

select2 = [ SELECT,
    [ ALL, [COLUMN, 'T1', 'number'],
           [COLUMN, 'T2', 'id'],
           [COLUMN, 'T2', 'first_name'],
           [COLUMN, 'T2', 'age']
    ],
    [ FROM, [ 'T1', TABLE, 'Groups' ],
            [ 'T2', TABLE, 'Students',
              [ EQ, [COLUMN, 'T1', 'number'], [COLUMN, 'T2', 'group'] ]
            ],
    ],
    [ WHERE, [ EQ, [COLUMN, 'T1', 'speciality'], [VALUE, 2201] ] ],
    [ ORDER_BY, [[COLUMN, 'T2', 'age'], DESC] ],
    [ LIMIT, [VALUE, 10] ]
]

# fnumber = 4
# query3 = select(
#     g for g in Groups(faculty=fnumber) if not g.students
# )

select3 = [ SELECT,
    [ ALL, [COLUMN, 'T1', 'number'],
           [COLUMN, 'T1', 'speciality'],
           # [COLUMN, 'T1', 'faculty']
    ],
    [ FROM, [ 'T1', TABLE, 'Groups' ] ],
    [ WHERE,
      [ AND,
        [ EQ, [COLUMN, 'T1', 'faculty'], [PARAM, 1] ],
        [ NOT_EXISTS,
          [ FROM, [ 'T2', TABLE, 'Students' ] ],
          [ WHERE, [ EQ, [COLUMN, 'T1', 'number'], [COLUMN, 'T2', 'group'] ] ]
        ],
      ]
    ]
]

################################################################################

expr_example_1 = [ AND,
    [ EQ, [COLUMN, 'T1', 'A'], [COLUMN, 'T2', 'B'] ],
    [ OR,
      [ LE, [COLUMN, 'T1', 'X'], [PARAM, 1] ],
      [ GT, [COLUMN, 'T2', 'Y'], [VALUE, 1000] ]
    ],
]

expr_example_2 = [ AND,
    [ BETWEEN, [COLUMN, 'T1', 'A'],
               [PARAM, 1],
               [ADD, [PARAM, 1], [VALUE, 100]]],
    [ LIKE, [COLUMN, 'T1', 'B'], [VALUE, 'A%'] ]
]

select_example_1 = [ SELECT,    # <-- May be SELECT, EXISTS, COUNT
    [ DISTINCT,                 # <-- May be DISTINCT, ALL, AGGREGATES
      [ COLUMN, 'T1', 'A' ],
      [ COLUMN, 'T2', 'C' ],
    ],
    [ FROM,                     # <-- FROM or LEFT_JOIN
      [ 'T1', TABLE, 'Table1'], # <-- TABLE or SELECT
      [ 'T2', TABLE, 'Table2', 
        [ EQ, [COLUMN, 'T1', 'C'], [COLUMN, 'T2', 'C'] ]
      ],
    ],
    [ WHERE, [ AND, [ EQ, [COLUMN, 'T1', 'X'], [PARAM, 1] ],
                    [ GT, [COLUMN, 'T2', 'Y'], [VALUE, 1000] ]
               ]
    ],
    [ UNION, ALL,
      [ ALL,
        [ COLUMN, 'T3', 'X' ],
        [ COLUMN, 'T3', 'Y' ]
      ],
      [ FROM,
        [ 'T3', TABLE, 'Table3' ]
      ],
      [ WHERE, [ EQ, [COLUMN, 'T3', 'Z'], [PARAM, 1] ] ],
    ],
    [ ORDER_BY, ([COLUMN, 'T2', 'C'], ASC),
                ([COLUMN, 'T1', 'A'], DESC)]
]
