from __future__ import annotations

import inspect
import types
from collections import defaultdict
from collections.abc import Callable, Generator, Iterable, Sequence
from opcode import (
    EXTENDED_ARG,
    HAVE_ARGUMENT,
    cmp_op,
    hascompare,
    hasconst,
    hasfree,
    hasjabs,
    hasjrel,
    haslocal,
    hasname,
)
from opcode import opname as opnames
from typing import TYPE_CHECKING, TypeAlias, cast

from pony.py23compat import (
    PY36,
    PY37,
    PY38,
    PY39,
    PY310,
    PY311,
    PY312,
    PY313,
    PY314,
    PYPY,
)

if TYPE_CHECKING:
    import _typeshed

try:
    from opcode import _nb_ops as nb_ops  # pyright: ignore[reportAttributeAccessIssue]
except ImportError:
    nb_ops = None

# from pony.thirdparty.compiler import ast, parse
import ast
import ast as ast_module

from pony.utils import get_codeobject_id, throw

##ast.And.__repr__ = lambda self: "And(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)
##ast.Or.__repr__ = lambda self: "Or(%s: %s)" % (getattr(self, 'endpos', '?'), repr(self.nodes),)

_ConstValue: TypeAlias = "str | bytes | bool | int | float | complex | None"


class DecompileError(NotImplementedError):
    pass


ast_cache: dict[int, tuple[ast.expr, set[str]]] = {}


def decompile(
    x: types.CodeType | Generator | Callable,
) -> tuple[ast.expr, set[str], dict[str, types.CellType]]:
    cells = {}
    t = type(x)
    codeobject = None
    if t is types.CodeType:
        assert isinstance(x, types.CodeType)
        codeobject = x
    elif t is types.GeneratorType:
        assert isinstance(x, types.GeneratorType)
        codeobject = x.gi_code
    elif t is types.FunctionType:
        assert isinstance(x, types.FunctionType)
        x = inspect.unwrap(x)
        assert isinstance(x, types.FunctionType)
        codeobject = x.__code__
        if x.__closure__:
            cells = dict(zip(codeobject.co_freevars, x.__closure__))
    else:
        throw(TypeError("Can't decompile %r" % t))
    assert isinstance(codeobject, types.CodeType)
    key = get_codeobject_id(codeobject)
    result: tuple[ast.expr, set[str]] | None = ast_cache.get(key)
    if result is None:
        decompiler = Decompiler(codeobject)
        result = decompiler.ast, decompiler.external_names
        ast_cache[key] = result
    return result + (cells,)


def simplify(clause: ast.expr) -> ast.expr:
    if isinstance(clause, ast.BoolOp) and isinstance(clause.op, ast.And):
        if len(clause.values) == 1:
            result = clause.values[0]
        else:
            return clause
    elif isinstance(clause, ast.BoolOp) and isinstance(clause.op, ast.Or):
        if len(clause.values) == 1:
            result = ast.UnaryOp(op=ast.Not(), operand=clause.values[0])
        else:
            return clause
    else:
        return clause
    if getattr(result, "endpos", 0) < clause.endpos:  # pyright: ignore[reportAttributeAccessIssue]
        result.endpos = clause.endpos  # pyright: ignore[reportAttributeAccessIssue]
    return result


class InvalidQuery(Exception):
    pass


def binop(node_type: type[ast.operator]) -> Callable:
    def method(decompiler: Decompiler) -> ast.BinOp:
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        return ast.BinOp(left=oper1, op=node_type(), right=oper2)

    return method


operator_mapping = {
    "==": ast.Eq,
    "!=": ast.NotEq,
    "<": ast.Lt,
    "<=": ast.LtE,
    ">": ast.Gt,
    ">=": ast.GtE,
    "is": ast.Is,
    "is not": ast.IsNot,
    "in": ast.In,
    "not in": ast.NotIn,
}

double_load_store_ops = {
    "LOAD_FAST_LOAD_FAST",
    "STORE_FAST_LOAD_FAST",
    "STORE_FAST_STORE_FAST",
    "LOAD_FAST_BORROW_LOAD_FAST_BORROW",
}

direct_jump_to_loop_ops = {
    "POP_JUMP_IF_FALSE": "POP_JUMP_BACKWARD_IF_FALSE",
    "POP_JUMP_IF_TRUE": "POP_JUMP_BACKWARD_IF_TRUE",
    "POP_JUMP_IF_NONE": "POP_JUMP_BACKWARD_IF_NONE",
    "POP_JUMP_IF_NOT_NONE": "POP_JUMP_BACKWARD_IF_NOT_NONE",
}


def unpack_targets(node: ast.Assign) -> ast.Tuple:
    # UNPACK_SEQUENCE creates an ``Assign`` pseudo node whose ``targets`` is not
    # a list but an ``ast.Tuple`` that collects the targets of the unpacking
    return cast("ast.Tuple", node.targets)


def clean_assign(node: ast.expr | ast.Assign) -> ast.expr:
    if isinstance(node, ast.Assign):
        return unpack_targets(node)
    return node


def make_const(
    value: _ConstValue | tuple[_ConstValue, ...] | tuple[tuple, ...] | slice,
) -> ast.Constant | ast.Tuple | ast.Slice:
    if is_const(value):  # pyright: ignore[reportArgumentType]
        assert isinstance(value, ast.Constant)
        return value
    if PY314:
        if isinstance(value, slice):
            return slice_to_ast(value)
    if PY39:
        return ast.Constant(value)  # pyright: ignore[reportArgumentType]
    elif PY38:
        return ast.Constant(value, kind=None)  # pyright: ignore[reportArgumentType]
    elif isinstance(value, (int, float)):
        return ast.Num(value)
    elif isinstance(value, str):
        return ast.Str(value)
    elif isinstance(value, bytes):
        return ast.Bytes(value)
    elif isinstance(value, tuple):
        return ast.Tuple([make_const(elt) for elt in value], ast.Load())
    elif value in (True, False, None):
        return ast.NameConstant(value)  # pyright: ignore[reportCallIssue]
    elif isinstance(value, types.CodeType):
        return ast.Constant(value)
    elif value is Ellipsis:
        if PY39:
            return ast.Constant(value)  # pyright: ignore[reportArgumentType]
        elif PY38:
            return ast.Constant(value, kind=None)  # pyright: ignore[reportArgumentType]
        else:
            return ast.Constant(value)  # pyright: ignore[reportArgumentType]
    assert False, value


def is_const(value: _ConstValue | tuple[_ConstValue, ...] | tuple[tuple, ...]) -> bool:
    if isinstance(value, ast.Constant):
        return True
    if PY38:
        return False
    if isinstance(value, (ast.Num, ast.Str, ast.Bytes)):
        return True
    if isinstance(value, ast.Tuple):
        return all(is_const(elt) for elt in value.elts)  # pyright: ignore[reportArgumentType]
    return False


def unwrap_str(key: str | ast.Str) -> str | ast._ConstantValue:
    if PY38:
        assert isinstance(key, str)
        return key
    assert isinstance(key, ast.Str)
    return key.s


def slice_to_ast(value: slice) -> ast.Slice:
    start = ast.Constant(value.start) if value.start is not None else None
    stop = ast.Constant(value.stop) if value.stop is not None else None
    step = ast.Constant(value.step) if value.step is not None else None
    return ast.Slice(start, stop, step)


class Decompiler:
    ast: ast.expr | _typeshed.MaybeNone
    abs_jump_to_top: int
    assnames: set[str]
    code: types.CodeType
    conditions_end: int
    end: int
    external_names: set[str]
    for_iter_pos: int
    instructions: list[tuple[int, int, str, list[int]]]
    instructions_map: dict[int, int]
    jump_map: defaultdict[int, list[int]]
    kw_names: Iterable[str] | None
    names: set[str]
    next_pos: int
    or_jumps: set[int]
    pos: int
    stack: list[ast.expr | _typeshed.MaybeNone]  # None marks a pushed NULL
    start: int
    targets: dict[int, ast.expr]

    def __init__(
        self, code: types.CodeType, start: int = 0, end: int | None = None
    ) -> None:
        self.code = code
        self.start = self.pos = start
        if end is None:
            end = len(code.co_code)
        self.end = end
        self.stack = []
        self.jump_map = defaultdict(list)
        self.targets = {}
        self.ast = None
        self.names = set()
        self.assnames = set()
        self.conditions_end = 0
        self.instructions = []
        self.instructions_map = {}
        self.kw_names = None
        self.or_jumps = set()
        self.get_instructions()
        self.analyze_jumps()
        self.decompile()
        self.ast = self.stack.pop()
        self.external_names = self.names - self.assnames
        if self.stack:
            throw(DecompileError, "Compiled code should represent a single expression")

    def get_instructions(self) -> None:
        before_yield = True
        code = self.code
        co_code = code.co_code
        free = code.co_cellvars + code.co_freevars
        self.abs_jump_to_top = self.for_iter_pos = -1
        while self.pos < self.end:
            i = self.pos
            op = code.co_code[i]
            oparg: int | None = None
            if PY36:
                extended_arg = 0
                oparg = code.co_code[i + 1]
                while op == EXTENDED_ARG:
                    extended_arg = (extended_arg | oparg) << 8
                    i += 2
                    op = code.co_code[i]
                    oparg = code.co_code[i + 1]
                oparg = None if op < HAVE_ARGUMENT else oparg | extended_arg
                i += 2
            else:
                i += 1
                if op >= HAVE_ARGUMENT:
                    oparg = co_code[i] + co_code[i + 1] * 256
                    i += 2
                    if op == EXTENDED_ARG:
                        op = code.co_code[i]
                        i += 1
                        oparg = co_code[i] + co_code[i + 1] * 256 + oparg * 65536
                        i += 2

            # CACHE bytes have inconvenient placement in py3.13, so we need to skip them
            while i < len(code.co_code) and opnames[code.co_code[i]] == "CACHE":
                i += 2

            opname = opnames[op].replace("+", "_")
            if op >= HAVE_ARGUMENT:
                assert oparg is not None
                if op in hasconst:
                    arg = [code.co_consts[oparg]]
                elif op in hasname:
                    if opname == "LOAD_GLOBAL":
                        push_null = False
                        if PY311:
                            push_null = oparg & 1
                            oparg >>= 1
                        arg = [code.co_names[oparg], push_null]
                    elif opname == "LOAD_ATTR":
                        push_null = False
                        if PY312:
                            push_null = oparg & 1
                            oparg >>= 1
                        arg = [code.co_names[oparg], push_null]
                    else:
                        arg = [code.co_names[oparg]]
                elif op in hasjrel:
                    arg = [
                        i
                        + oparg
                        * (2 if PY310 else 1)
                        * (-1 if "BACKWARD" in opname else 1)
                    ]
                elif op in haslocal:
                    if opname in double_load_store_ops:
                        # py3.13: 2 4bit args
                        arg = [
                            code._varname_from_oparg(oparg >> 4),  # pyright: ignore[reportAttributeAccessIssue]
                            code._varname_from_oparg(oparg & 0x0F),  # pyright: ignore[reportAttributeAccessIssue]
                        ]
                    elif PY313:
                        # co_varnames is incomplete now
                        arg = [code._varname_from_oparg(oparg)]  # pyright: ignore[reportAttributeAccessIssue]
                    else:
                        arg = [code.co_varnames[oparg]]
                elif op in hascompare:
                    if PY313:
                        oparg >>= 5
                    elif PY312:
                        oparg >>= 4
                    arg = [cmp_op[oparg]]
                elif op in hasfree:
                    if PY311:
                        oparg -= len(code.co_varnames)
                    arg = [free[oparg]]
                elif op in hasjabs:
                    arg = [oparg * (2 if PY310 else 1)]
                else:
                    arg = [oparg]
            elif (
                PY313
                and opname == "MAKE_FUNCTION"
                and opnames[code.co_code[i]] == "SET_FUNCTION_ATTRIBUTE"
            ):
                # pull attributes from next instruction
                arg = [code.co_code[i + 1]]
                i += 2
            else:
                arg = []
            if opname == "FOR_ITER":
                self.for_iter_pos = self.pos
            if (
                opname in ("JUMP_ABSOLUTE", "JUMP_NO_INTERRUPT")
                and arg[0] == self.for_iter_pos
            ):
                self.abs_jump_to_top = self.pos

            # NOT_TAKEN has a similar problem as CACHE, but does not change rel jump addresses, so needs to be here
            while i < len(code.co_code) and opnames[code.co_code[i]] == "NOT_TAKEN":
                i += 2

            if before_yield:
                merge = False
                if opname == "JUMP_BACKWARD":
                    # in py 3.12 we have jump_if_true forward for yield
                    # and unconditional jump_backward for loop, so we
                    # fixup previous instruction to match pre-3.12,
                    # which is conditional jump backward for loop
                    # and fall through for yield
                    prev: list = list(self.instructions[-1])
                    endpos = arg[0]
                    assert isinstance(endpos, int)
                    merge = True
                    if prev[2] == "POP_JUMP_IF_TRUE":
                        prev[2] = "POP_JUMP_BACKWARD_IF_FALSE"
                    elif prev[2] == "POP_JUMP_IF_FALSE":
                        prev[2] = "POP_JUMP_BACKWARD_IF_TRUE"
                    elif prev[2] == "POP_JUMP_IF_NOT_NONE":
                        prev[2] = "POP_JUMP_BACKWARD_IF_NONE"
                    elif prev[2] == "POP_JUMP_IF_NONE":
                        prev[2] = "POP_JUMP_BACKWARD_IF_NOT_NONE"
                    elif prev[2] == "LIST_APPEND":
                        merge = False
                    else:
                        raise DecompileError(
                            f"Unsupported instruction combination: {prev[2]} + {opname}"
                        )
                    if merge:
                        prev[3] = cast("list[int]", prev[3])
                        old_endpos = prev[3][0]
                        prev[1] = i
                        prev[3] = arg
                        self.instructions[-1] = tuple(prev)
                        if endpos < self.pos:
                            self.conditions_end = i
                        self.jump_map[old_endpos].remove(prev[0])
                        self.jump_map[endpos].append(prev[0])
                if not merge:
                    if "JUMP" in opname:
                        endpos = arg[0]
                        assert isinstance(endpos, int)
                        if endpos < self.pos:
                            self.conditions_end = i
                        self.jump_map[endpos].append(self.pos)
                    self.instructions_map[self.pos] = len(self.instructions)
                    self.instructions.append((self.pos, i, opname, arg))  # pyright: ignore[reportArgumentType]
            elif PY312 and not PY313 and not PYPY and opname == "JUMP_BACKWARD":
                # In py3.12 multiline generator expressions conditional jumps can point
                # directly to the common JUMP_BACKWARD after YIELD_VALUE
                jump_target = arg[0]
                assert isinstance(jump_target, int)
                jump_starts = self.jump_map.get(self.pos, [])
                for jump_start in list(jump_starts):
                    instruction_index = self.instructions_map[jump_start]
                    instruction = self.instructions[instruction_index]
                    pos, next_pos, jump_opname, _ = instruction
                    backward_opname = direct_jump_to_loop_ops.get(jump_opname)
                    if backward_opname is None:
                        continue
                    self.instructions[instruction_index] = (
                        pos,
                        next_pos,
                        backward_opname,
                        [jump_target],
                    )
                    jump_starts.remove(jump_start)
                    self.jump_map[jump_target].append(jump_start)
                    self.conditions_end = max(self.conditions_end, next_pos)
            if opname == "YIELD_VALUE":
                before_yield = False
            self.pos = i

    def analyze_jumps(self) -> None:
        if PYPY:
            targets = self.jump_map.pop(self.abs_jump_to_top, [])
            self.jump_map[self.for_iter_pos] = targets
            for i, (x, y, opname, arg) in enumerate(self.instructions):
                if "JUMP" in opname:
                    target = arg[0]
                    if target == self.abs_jump_to_top:
                        self.instructions[i] = (
                            x,
                            y,
                            opname,
                            [self.for_iter_pos],
                        )
                        self.conditions_end = y

        i = self.instructions_map[self.conditions_end]
        while i > 0:
            pos, next_pos, opname, arg = self.instructions[i]
            if pos in self.jump_map:
                for jump_start_pos in self.jump_map[pos]:
                    if jump_start_pos > pos:
                        continue
                    for or_jump_start_pos in self.or_jumps:
                        if pos > or_jump_start_pos > jump_start_pos:
                            break  # And jump
                    else:
                        self.or_jumps.add(jump_start_pos)
            i -= 1

    def decompile(self) -> None:
        for pos, next_pos, opname, arg in self.instructions:
            if pos in self.targets:
                self.process_target(pos)
            method = getattr(self, opname, None)
            if method is None:
                throw(DecompileError("Unsupported operation: %s" % opname))
            self.pos = pos
            self.next_pos = next_pos
            x = method(*arg)
            if x is not None:
                self.stack.append(x)

    def pop_items(self, size: int) -> list[ast.expr]:
        if not size:
            return []
        result = self.stack[-size:]
        self.stack[-size:] = []
        return result

    def store(self, node: ast.expr | ast.Assign) -> None:
        stack = self.stack
        if not stack:
            stack.append(node)
            return
        top = stack[-1]
        if isinstance(top, ast.Assign):
            target = unpack_targets(top)
            if (
                isinstance(target, (ast.Tuple, ast.List))
                and len(target.elts) < top.count  # pyright: ignore[reportAttributeAccessIssue]
            ):
                target.elts.append(clean_assign(node))
                if len(target.elts) == top.count:  # pyright: ignore[reportAttributeAccessIssue]
                    self.store(stack.pop())
            else:
                stack.append(node)
        elif isinstance(top, ast.comprehension):
            assert top.target is None
            top.target = clean_assign(node)
        else:
            stack.append(node)

    BINARY_POWER = binop(ast_module.Pow)
    BINARY_MULTIPLY = binop(ast_module.Mult)
    BINARY_DIVIDE = binop(ast_module.Div)
    BINARY_FLOOR_DIVIDE = binop(ast_module.FloorDiv)
    BINARY_ADD = binop(ast_module.Add)
    BINARY_SUBTRACT = binop(ast_module.Sub)
    BINARY_LSHIFT = binop(ast_module.LShift)
    BINARY_RSHIFT = binop(ast_module.RShift)
    BINARY_AND = binop(ast_module.BitAnd)
    BINARY_XOR = binop(ast_module.BitXor)
    BINARY_OR = binop(ast_module.BitOr)
    BINARY_TRUE_DIVIDE = BINARY_DIVIDE
    BINARY_MODULO = binop(ast_module.Mod)

    def BINARY_OP(self, opcode: int | str) -> ast.BinOp | ast.Name | ast.Subscript:
        assert nb_ops is not None
        opname, symbol = nb_ops[opcode]
        inplace = opname.startswith("NB_INPLACE_")
        opname = opname.split("_", 2 if inplace else 1)[-1]

        if opname == "SUBSCR":
            return self.BINARY_SUBSCR()

        op = {
            "ADD": ast.Add,
            "AND": ast.BitAnd,
            "FLOOR_DIVIDE": ast.FloorDiv,
            "LSHIFT": ast.LShift,
            "MATRIX_MULTIPLY": ast.MatMult,
            "MULTIPLY": ast.Mult,
            "REMAINDER": ast.Mod,
            "OR": ast.BitOr,
            "POWER": ast.Pow,
            "RSHIFT": ast.RShift,
            "SUBTRACT": ast.Sub,
            "TRUE_DIVIDE": ast.Div,
            "XOR": ast.BitXor,
        }[opname]

        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        r = ast.BinOp(left=oper1, op=op(), right=oper2)
        if inplace:
            r = ast.Name(oper1, r)  # pyright: ignore[reportArgumentType]
        return r

    def BINARY_SLICE(self) -> ast.Subscript:
        # 3.12 optimized BUILD_SLICE + BINARY_SUBSCR
        end = self.stack.pop()
        start = self.stack.pop()
        node1 = self.stack.pop()
        if isinstance(end, ast.Constant) and end.value is None:
            end = None
        if isinstance(start, ast.Constant) and start.value is None:
            start = None
        node2 = ast.Slice(start, end)
        return ast.Subscript(value=node1, slice=node2, ctx=ast.Load())

    def BINARY_SUBSCR(self) -> ast.Subscript:
        node2 = self.stack.pop()
        node1 = self.stack.pop()
        if isinstance(node2, ast.Slice):  # and len(node2.nodes) == 2:
            if isinstance(node2.lower, ast.Constant) and node2.lower.value is None:
                node2.lower = None
            if isinstance(node2.upper, ast.Constant) and node2.upper.value is None:
                node2.upper = None
        elif (
            isinstance(node2, ast.Constant)
            and isinstance(node2.value, tuple)
            and any(isinstance(value, slice) for value in node2.value)
        ):
            # py3.14 has a different format for constant tuple of slices
            node2 = ast.Tuple(elts=[slice_to_ast(value) for value in node2.value])  # pyright: ignore[reportArgumentType]
        elif not PY38:
            if isinstance(node2, ast.Tuple) and any(
                isinstance(item, ast.Slice) for item in node2.elts
            ):
                node2 = ast.ExtSlice(node2.elts)  # pyright: ignore[reportArgumentType]
            else:
                node2 = ast.Index(node2)
        return ast.Subscript(value=node1, slice=node2, ctx=ast.Load())  # pyright: ignore[reportArgumentType]

    def BUILD_CONST_KEY_MAP(self, length: int) -> ast.Dict:
        keys = self.stack.pop()
        if PY38:
            assert isinstance(keys, ast.Constant), keys
            assert isinstance(keys.value, (tuple, list))
            keys = [make_const(key) for key in keys.value]
        else:
            assert isinstance(keys, ast.Tuple) and is_const(keys), keys  # pyright: ignore[reportArgumentType]
            keys = [make_const(key) for key in keys.elts]  # pyright: ignore[reportArgumentType]

        values = self.pop_items(length)
        return ast.Dict(keys=keys, values=values)  # pyright: ignore[reportArgumentType]

    def BUILD_LIST(self, size: int) -> ast.List:
        return ast.List(self.pop_items(size), ast.Load())

    def BUILD_MAP(self, length: int) -> ast.Dict:
        data = self.pop_items(2 * length)  # [key1, value1, key2, value2, ...]
        keys: list[ast.expr | None] = []
        values: list[ast.expr] = []
        for i in range(0, len(data), 2):
            keys.append(data[i])
            values.append(data[i + 1])
        return ast.Dict(keys=keys, values=values)

    def BUILD_SET(self, size: int) -> ast.Set:
        return ast.Set(self.pop_items(size))

    def BUILD_SLICE(self, size: int) -> ast.Slice:
        items = self.pop_items(size)
        if not PY38:
            items = [
                None
                if isinstance(item, ast.NameConstant) and item.value is None
                else item
                for item in items
            ]
        items += [None] * (3 - len(items))
        return ast.Slice(*items, ctx=ast.Load())

    def BUILD_TUPLE(self, size: int) -> ast.Tuple:
        return ast.Tuple(self.pop_items(size), ast.Load())

    def BUILD_STRING(self, count: int) -> ast.JoinedStr:
        items = list(reversed([self.stack.pop() for _ in range(count)]))
        for i, item in enumerate(items):
            if isinstance(item, ast.Constant):
                if not isinstance(item.value, str):
                    throw(NotImplementedError, item)
            elif not isinstance(item, ast.FormattedValue):
                items[i] = ast.FormattedValue(item, -1)
        return ast.JoinedStr(items)

    def CALL_FUNCTION(
        self, argc: int, star: ast.expr | None = None, star2: ast.expr | None = None
    ) -> ast.Call | ast.GeneratorExp:
        pop = self.stack.pop
        kwarg, posarg = divmod(argc, 256)
        keywords = []
        for _i in range(kwarg):
            arg = pop()
            key = cast("ast.Constant", pop()).value
            keywords.append(ast.keyword(unwrap_str(key), arg))  # pyright: ignore[reportArgumentType]
        keywords.reverse()
        args = []
        for _i in range(posarg):
            args.append(pop())
        args.reverse()
        if star:
            args.append(ast.Starred(value=star))
        if star2:
            keywords.append(ast.keyword(value=star2))
        return self._call_function(args, keywords)

    def _call_function(
        self,
        args: Iterable[ast.expr],
        keywords: Iterable[ast.keyword] | None = None,
    ) -> ast.GeneratorExp | ast.Call:
        tos = self.stack.pop()
        if isinstance(tos, ast.GeneratorExp):
            assert len(args) == 1 and not keywords  # pyright: ignore[reportArgumentType]
            genexpr = tos
            qual = genexpr.generators[0]
            assert isinstance(qual.iter, ast.Name)
            assert qual.iter.id == ".0"
            assert isinstance(args, Sequence)
            qual.iter = args[0]
            return genexpr
        return ast.Call(tos, args, keywords)  # pyright: ignore[reportArgumentType]

    def CACHE(self) -> None:
        pass

    def CALL(self, argc: int) -> ast.Call | ast.GeneratorExp:
        values = self.pop_items(argc)

        keys = self.kw_names
        self.kw_names = None

        args = values
        keywords = []
        if keys:
            args = values[: -len(keys)]  # pyright: ignore[reportArgumentType]
            keywords = [ast.keyword(k, v) for k, v in zip(keys, values[-len(keys) :])]  # pyright: ignore[reportArgumentType]

        if PY313:
            # self/NULL and callable are swapped
            self_ = self.stack.pop()
            callable_ = self.stack.pop()
            if self_ is not None:
                args.insert(0, self_)
        else:
            self_ = self.stack.pop()
            callable_ = self.stack.pop()
            if callable_ is None:
                callable_ = self_
            else:
                args.insert(0, self_)
        self.stack.append(callable_)
        return self._call_function(args, keywords)

    def CALL_FUNCTION_VAR(self, argc: int) -> ast.Call | ast.GeneratorExp:
        return self.CALL_FUNCTION(argc, self.stack.pop())

    def CALL_FUNCTION_KW(self, argc: int) -> ast.Call | ast.GeneratorExp:
        keys = self.stack.pop()
        assert is_const(keys), keys  # pyright: ignore[reportArgumentType]
        if PY38:
            assert isinstance(keys, ast.Constant)
            keys = keys.value
        else:
            assert isinstance(keys, ast.Tuple)
            keys = keys.elts
        values = self.pop_items(argc)
        assert len(keys) <= len(values)  # pyright: ignore[reportArgumentType]
        args = values[: -len(keys)]  # pyright: ignore[reportArgumentType]
        keywords = [
            ast.keyword(unwrap_str(k), v)  # pyright: ignore[reportArgumentType]
            for k, v in zip(keys, values[-len(keys) :])  # pyright: ignore[reportArgumentType]
        ]
        return self._call_function(args, keywords)

    def CALL_FUNCTION_VAR_KW(self, argc: int) -> ast.Call | ast.GeneratorExp:
        star2 = self.stack.pop()
        star = self.stack.pop()
        return self.CALL_FUNCTION(argc, star, star2)

    def CALL_FUNCTION_EX(self, argc: int = 1) -> ast.Call | ast.GeneratorExp:
        star2 = None
        if argc:
            if argc != 1:
                throw(DecompileError)
            star2 = self.stack.pop()
        star = self.stack.pop()
        args = [ast.Starred(value=star)] if star else None
        keywords = [ast.keyword(value=star2)] if star2 else None
        if PY313:
            # self/NULL and callable are swapped; FIXME: this leaves NULL on the stack?
            self_ = self.stack.pop()
            callable_ = self.stack.pop()
            self.stack.append(self_)
            self.stack.append(callable_)
        return self._call_function(args, keywords)  # pyright: ignore[reportArgumentType]

    def CALL_METHOD(self, argc: int) -> ast.Call:
        pop = self.stack.pop
        args = []
        keywords = []
        if argc >= 256:
            kwargc = argc // 256
            argc = argc % 256
            for _i in range(kwargc):
                v = pop()
                k = pop()
                assert isinstance(k, ast.Constant)
                k = k.value  # ast.Name(k.value)
                keywords.append(ast.keyword(k, v))  # pyright: ignore[reportArgumentType]
        for _i in range(argc):
            args.append(pop())
        args.reverse()
        method = pop()
        return ast.Call(method, args, keywords)

    def CALL_KW(self, argc: int) -> ast.Call | ast.GeneratorExp:
        names = self.stack.pop()
        if isinstance(names, ast.Constant):
            self.kw_names = cast("Iterable[str]", names.value)
        else:
            raise NotImplementedError(
                f"CALL_KW for {names.__class__.__name__} not implemented"
            )
        return self.CALL(argc)

    def COMPARE_OP(self, op: str) -> ast.Compare:
        oper2 = self.stack.pop()
        oper1 = self.stack.pop()
        op_map = operator_mapping[op]()
        return ast.Compare(oper1, [op_map], [oper2])

    def CONVERT_VALUE(self, conversion: int) -> tuple[ast.expr, int]:
        value = self.stack.pop()
        return value, [-1, ord("s"), ord("r"), ord("a")][conversion]

    def COPY(self, _):
        pass  # this is not great, but stack is not the same as during runtime
        # actual queries are hopefully covered by tests

    def COPY_FREE_VARS(self, n: int) -> None:
        pass

    def CONTAINS_OP(self, invert: int) -> ast.Compare:
        return self.COMPARE_OP("not in" if invert else "in")

    def DUP_TOP(self) -> ast.expr:
        return self.stack[-1]

    def FOR_ITER(self, endpos: int) -> ast.comprehension:
        target = None
        iter = self.stack.pop()
        ifs: list[ast.expr] = []
        return ast.comprehension(target, iter, ifs, 0)  # pyright: ignore[reportArgumentType]

    def FORMAT_VALUE(self, flags: int) -> ast.FormattedValue:
        conversion = -1
        format_spec = None
        if flags in (0, 1, 2, 3):
            value = self.stack.pop()
            if flags == 0:
                conversion = -1
            elif flags == 1:
                conversion = ord("s")  # str conversion
            elif flags == 2:
                conversion = ord("r")  # repr conversion
            elif flags == 3:
                conversion = ord("a")  # ascii conversion
        elif flags == 4:
            format_spec = self.stack.pop()
            value = self.stack.pop()
        return ast.FormattedValue(
            value=value,  # pyright: ignore[reportPossiblyUnboundVariable]
            conversion=conversion,
            format_spec=format_spec,
        )

    def FORMAT_SIMPLE(self) -> ast.FormattedValue:
        # see CONVERT_VALUE
        args = self.stack.pop()
        if isinstance(args, tuple):
            value, conversion = args
        else:
            value, conversion = args, -1
        return ast.FormattedValue(value=value, conversion=conversion)

    def FORMAT_WITH_SPEC(self) -> ast.FormattedValue:
        spec = self.stack.pop()
        args = self.stack.pop()
        if isinstance(args, tuple):
            # FIXME: is this correct here? should we look for ast.FormattedValue instead?
            value, conversion = args
        else:
            value, conversion = args, -1
        return ast.FormattedValue(value=value, conversion=conversion, format_spec=spec)

    def GEN_START(self, kind: int) -> None:
        assert kind == 0  # only support sync

    def GET_ITER(self) -> None:
        pass

    def JUMP_IF_FALSE(self, endpos: int) -> ast.BoolOp:
        return self.conditional_jump(endpos, False)

    JUMP_IF_FALSE_OR_POP = JUMP_IF_FALSE

    def JUMP_IF_NOT_EXC_MATCH(self, endpos: int) -> ast.BoolOp:
        raise NotImplementedError

    def JUMP_IF_TRUE(self, endpos: int) -> ast.BoolOp:
        return self.conditional_jump(endpos, True)

    JUMP_IF_TRUE_OR_POP = JUMP_IF_TRUE

    def conditional_jump(self, endpos: int, if_true: bool) -> ast.BoolOp:
        if PY37 or PYPY:
            return self.conditional_jump_new(endpos, if_true)
        return self.conditional_jump_old(endpos, if_true)

    def conditional_jump_old(self, endpos: int, if_true: bool) -> ast.BoolOp:
        i = self.next_pos
        if i in self.targets:
            self.process_target(i)
        expr = self.stack.pop()
        clausetype = ast.Or if if_true else ast.And
        clause = ast.BoolOp(op=clausetype(), values=[expr])
        clause.endpos = endpos  # pyright: ignore[reportAttributeAccessIssue]
        self.targets.setdefault(endpos, clause)
        return clause

    def conditional_jump_new(self, endpos: int, if_true: bool) -> ast.BoolOp:
        expr = self.stack.pop()
        if self.pos >= self.conditions_end:
            clausetype = ast.Or if if_true else ast.And
        elif self.pos in self.or_jumps:
            clausetype = ast.Or
            if not if_true:
                expr = ast.UnaryOp(op=ast.Not(), operand=expr)
        else:
            clausetype = ast.And
            if if_true:
                expr = ast.UnaryOp(op=ast.Not(), operand=expr)
        self.stack.append(expr)

        if self.next_pos in self.targets:
            self.process_target(self.next_pos)

        expr = self.stack.pop()
        clause = ast.BoolOp(op=clausetype(), values=[expr])
        clause.endpos = endpos  # pyright: ignore[reportAttributeAccessIssue]
        self.targets.setdefault(endpos, clause)
        return clause

    def conditional_jump_none_impl(self, endpos: int, negate: bool) -> ast.BoolOp:
        expr = self.stack.pop()
        assert self.pos < self.conditions_end
        if self.pos in self.or_jumps:
            clausetype = ast.Or
            op = ast.IsNot if negate else ast.Is
        else:
            clausetype = ast.And
            op = ast.Is if negate else ast.IsNot
        expr = ast.Compare(expr, [op()], [ast.Constant(None)])
        self.stack.append(expr)

        if self.next_pos in self.targets:
            self.process_target(self.next_pos)

        expr = self.stack.pop()
        clause = ast.BoolOp(op=clausetype(), values=[expr])
        clause.endpos = endpos  # pyright: ignore[reportAttributeAccessIssue]
        self.targets.setdefault(endpos, clause)
        return clause

    def jump_if_none(self, endpos: int) -> ast.BoolOp:
        return self.conditional_jump_none_impl(endpos, False)

    def jump_if_not_none(self, endpos: int) -> ast.BoolOp:
        return self.conditional_jump_none_impl(endpos, True)

    def POP_JUMP_IF_NONE(self, endpos: int) -> ast.BoolOp:
        return self.jump_if_none(endpos)

    def POP_JUMP_IF_NOT_NONE(self, endpos: int) -> ast.BoolOp:
        return self.jump_if_not_none(endpos)

    def process_target(self, pos: int | None, partial: bool = False) -> None:
        if pos is None:
            limit = None
        elif partial:
            limit = self.targets.get(pos, None)
        else:
            limit = self.targets.pop(pos, None)
        top = self.stack.pop()
        while True:
            top = simplify(top)
            if top is limit:
                break
            if isinstance(top, ast.comprehension):
                break
            if not self.stack:
                break
            if self.stack[-1] is None:
                self.stack.pop()
                if not self.stack:
                    break
            top2 = self.stack[-1]
            if isinstance(top2, ast.comprehension):
                break
            if partial and hasattr(top2, "endpos") and top2.endpos == pos:  # pyright: ignore[reportAttributeAccessIssue, reportOptionalMemberAccess]
                break

            if isinstance(top2, ast.BoolOp):
                if isinstance(top, ast.BoolOp) and type(top2.op) is type(top.op):
                    top2.values.extend(top.values)
                else:
                    top2.values.append(top)
            elif isinstance(top2, ast.IfExp):  # Python 2.5
                top2.orelse = top
                if hasattr(top, "endpos"):
                    top2.endpos = top.endpos  # pyright: ignore[reportAttributeAccessIssue]
                    if self.targets.get(top.endpos) is top:  # pyright: ignore[reportAttributeAccessIssue]
                        self.targets[top.endpos] = top2  # pyright: ignore[reportAttributeAccessIssue]
            else:
                throw(
                    DecompileError(
                        "Expression is too complex to decompile, try to pass query as string, "
                        'e.g. select("x for x in Something")'
                    )
                )
            top2.endpos = max(top2.endpos, getattr(top, "endpos", 0))  # pyright: ignore[reportAttributeAccessIssue]
            top = self.stack.pop()
        self.stack.append(top)

    def JUMP_FORWARD(self, endpos: int) -> ast.IfExp:
        i = self.next_pos  # next instruction
        self.process_target(i, True)
        then = self.stack.pop()
        self.process_target(i, False)
        test = self.stack.pop()
        if_exp = ast.IfExp(test=simplify(test), body=simplify(then), orelse=None)  # pyright: ignore[reportArgumentType]
        if_exp.endpos = endpos  # pyright: ignore[reportAttributeAccessIssue]
        self.targets.setdefault(endpos, if_exp)
        if self.targets.get(endpos) is then:
            self.targets[endpos] = if_exp
        return if_exp

    def KW_NAMES(self, kw_names: Iterable[str]) -> None:
        # Stash for CALL
        self.kw_names = kw_names

    def IS_OP(self, invert: int) -> ast.Compare:
        return self.COMPARE_OP("is not" if invert else "is")

    def LIST_APPEND(self, offset: int) -> None:
        tos = self.stack.pop()
        list_node = self.stack[-offset]
        if isinstance(list_node, ast.comprehension):
            throw(
                InvalidQuery(
                    "Use generator expression (... for ... in ...) "
                    "instead of list comprehension [... for ... in ...] inside query"
                )
            )

        assert isinstance(list_node, ast.List), list_node
        list_node.elts.append(tos)

    def LIST_EXTEND(self, offset: int) -> ast.List:
        if offset != 1:
            raise NotImplementedError(offset)
        items = self.stack.pop()
        if not isinstance(items, ast.Constant):
            raise NotImplementedError(type(items))
        if not isinstance(items.value, tuple):
            raise NotImplementedError(type(items.value))
        lst = self.stack.pop()
        if not isinstance(lst, ast.List):
            raise NotImplementedError(type(lst))
        values = [make_const(v) for v in items.value]
        lst.elts.extend(values)
        return lst

    def LIST_TO_TUPLE(self) -> ast.Tuple:
        tos = self.stack.pop()
        if not isinstance(tos, ast.List):
            throw(
                InvalidQuery,
                "Translation error, please contact developers: list expected, got: %r"
                % tos,
            )
        return ast.Tuple(tos.elts, ast.Load())

    def LOAD_ATTR(self, attr_name: str, push_null: bool) -> ast.Attribute | None:
        res = ast.Attribute(self.stack.pop(), attr_name, ast.Load())
        if push_null and PY313:
            # NULL and attr swapped
            self.stack.append(res)
            self.stack.append(None)
            return None
        elif push_null:
            self.stack.append(None)
        return res

    def LOAD_CLOSURE(self, freevar: str) -> ast.Name:
        self.names.add(freevar)
        return ast.Name(freevar, ast.Load())

    def LOAD_CONST(
        self,
        const_value: _ConstValue | tuple[_ConstValue, ...] | tuple[tuple, ...] | slice,
    ) -> ast.Constant | ast.Tuple | ast.Slice:
        return make_const(const_value)

    def LOAD_DEREF(self, freevar: str) -> ast.Name:
        self.names.add(freevar)
        return ast.Name(freevar, ast.Load())

    def LOAD_FAST(self, varname: str) -> ast.Name:
        self.names.add(varname)
        return ast.Name(varname, ast.Load())

    LOAD_FAST_AND_CLEAR = LOAD_FAST
    LOAD_FAST_BORROW = LOAD_FAST

    def LOAD_FAST_LOAD_FAST(self, varname1: str, varname2: str) -> ast.Name:
        self.names.add(varname1)
        self.stack.append(ast.Name(varname1, ast.Load()))
        return self.LOAD_FAST(varname2)

    LOAD_FAST_BORROW_LOAD_FAST_BORROW = LOAD_FAST_LOAD_FAST

    def LOAD_GLOBAL(self, varname: str, push_null: int) -> ast.Name | None:
        res = ast.Name(varname, ast.Load())
        self.names.add(varname)
        if push_null and PY313:
            # NULL and global swapped
            self.stack.append(res)
            self.stack.append(None)
            return None
        elif push_null and not PY313:
            self.stack.append(None)
        return res

    def LOAD_METHOD(self, methname: str) -> ast.Attribute | None:
        return self.LOAD_ATTR(methname, PY311)

    LOOKUP_METHOD = LOAD_METHOD  # For PyPy

    def LOAD_NAME(self, varname: str) -> ast.Name:
        self.names.add(varname)
        return ast.Name(varname, ast.Load())

    def LOAD_SMALL_INT(self, value: int) -> ast.Constant | ast.Tuple | ast.Slice:
        return make_const(value)

    def MAKE_CELL(self, freevar: str) -> None:
        pass

    def MAKE_CLOSURE(self, argc: int) -> ast.expr:
        self.stack[-3:-2] = []  # ignore freevars
        return self.MAKE_FUNCTION(argc)

    def MAKE_FUNCTION(self, argc: int = 0) -> ast.expr:
        defaults = []
        if not PY311:
            self.stack.pop()  # qualname
        tos = cast("ast.Constant", self.stack.pop())
        if argc & 0x08:
            self.stack.pop()  # function closure
        if argc & 0x04:
            self.stack.pop()  # annotations
        if argc & 0x02:
            self.stack.pop()  # keyword-only defaults
            throw(NotImplementedError)
        if argc & 0x01:
            defaults = self.stack.pop()
            assert isinstance(defaults, ast.Tuple)
            defaults = defaults.elts
        codeobject = tos.value
        func_decompiler = Decompiler(codeobject)  # pyright: ignore[reportArgumentType]
        # decompiler.names.update(decompiler.names)  ???
        if codeobject.co_varnames[:1] == (".0",):  # pyright: ignore[reportAttributeAccessIssue, reportOptionalMemberAccess]
            return func_decompiler.ast  # generator
        argnames, vararg, kwarg = inspect.getargs(codeobject)  # pyright: ignore[reportArgumentType]
        args = ast.arguments(
            posonlyargs=[],
            args=[ast.arg(arg=v) for v in argnames],
            kwonlyargs=[],
            kw_defaults=[],
            defaults=defaults,
            vararg=ast.arg(arg=vararg) if vararg else None,
            kwarg=ast.arg(arg=kwarg) if kwarg else None,
        )
        return ast.Lambda(args, func_decompiler.ast)

    POP_JUMP_BACKWARD_IF_FALSE = JUMP_IF_FALSE
    POP_JUMP_BACKWARD_IF_TRUE = JUMP_IF_TRUE
    POP_JUMP_FORWARD_IF_FALSE = JUMP_IF_FALSE
    POP_JUMP_FORWARD_IF_TRUE = JUMP_IF_TRUE
    POP_JUMP_IF_FALSE = JUMP_IF_FALSE
    POP_JUMP_IF_TRUE = JUMP_IF_TRUE
    POP_JUMP_BACKWARD_IF_NONE = jump_if_none
    POP_JUMP_BACKWARD_IF_NOT_NONE = jump_if_not_none
    POP_JUMP_FORWARD_IF_NONE = jump_if_none
    POP_JUMP_FORWARD_IF_NOT_NONE = jump_if_not_none

    def POP_TOP(self) -> None:
        pass

    def PRECALL(self, argc: int) -> None:
        pass

    def PUSH_NULL(self) -> None:
        self.stack.append(None)

    def RETURN_VALUE(self) -> ast.expr:
        if self.next_pos != self.end:
            throw(DecompileError)
        expr = self.stack.pop()
        return simplify(expr)

    def RETURN_CONST(
        self, val: _ConstValue | tuple[_ConstValue, ...] | tuple[tuple, ...] | slice
    ) -> ast.Constant | ast.Tuple | ast.Slice:
        return make_const(val)

    def RETURN_GENERATOR(self) -> None:
        pass

    def RESUME(self, where: int) -> None:
        pass

    def ROT_TWO(self) -> None:
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        self.stack.append(tos)
        self.stack.append(tos1)

    def ROT_THREE(self) -> None:
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack.pop()
        self.stack.append(tos)
        self.stack.append(tos2)
        self.stack.append(tos1)

    def SET_FUNCTION_ATTRIBUTE(self, flag):
        """This replaces the argument to MAKE_FUNCTION in py 3.13"""
        # This actually needs special handling in get_instructions because
        # the func will not be at the top of the stack when we get here.
        throw(NotImplementedError)

    def SETUP_LOOP(self, endpos: int):
        pass

    def STORE_ATTR(self, attrname: str) -> None:
        self.store(ast.Attribute(self.stack.pop(), attrname, ast.Store()))

    def STORE_DEREF(self, freevar: str) -> None:
        self.assnames.add(freevar)
        self.store(ast.Name(freevar, ast.Store()))

    def STORE_FAST(self, varname: str) -> None:
        if varname.startswith("_["):
            throw(
                InvalidQuery(
                    "Use generator expression (... for ... in ...) "
                    "instead of list comprehension [... for ... in ...] inside query"
                )
            )
        self.assnames.add(varname)
        self.store(ast.Name(varname, ast.Store()))

    def STORE_FAST_STORE_FAST(self, varname1: str, varname2: str) -> None:
        self.STORE_FAST(varname1)
        self.STORE_FAST(varname2)

    def STORE_FAST_LOAD_FAST(self, varname1: str, varname2: str) -> ast.Name:
        self.STORE_FAST(varname1)
        return self.LOAD_FAST(varname2)

    def STORE_MAP(self) -> None:
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack[-1]
        if not isinstance(tos2, ast.Dict):
            assert False  # pragma: no cover
        if tos2.items == ():  # pyright: ignore[reportAttributeAccessIssue]
            tos2.items = []  # pyright: ignore[reportAttributeAccessIssue]
        tos2.items.append((tos, tos1))  # pyright: ignore[reportAttributeAccessIssue]

    def STORE_SUBSCR(self) -> None:
        tos = self.stack.pop()
        tos1 = self.stack.pop()
        tos2 = self.stack.pop()
        if not isinstance(tos1, ast.Dict):
            assert False  # pragma: no cover
        if tos1.items == ():  # pyright: ignore[reportAttributeAccessIssue]
            tos1.items = []  # pyright: ignore[reportAttributeAccessIssue]
        tos1.items.append((tos, tos2))  # pyright: ignore[reportAttributeAccessIssue]

    def SWAP(self, _):
        pass  # this is not great, but stack is not the same as during runtime
        # actual queries are hopefully covered by tests

    def TO_BOOL(self):
        pass

    def UNARY_POSITIVE(self) -> ast.UnaryOp:
        return ast.UnaryOp(op=ast.UAdd(), operand=self.stack.pop())

    def UNARY_NEGATIVE(self) -> ast.UnaryOp:
        return ast.UnaryOp(op=ast.USub(), operand=self.stack.pop())

    def UNARY_NOT(self) -> ast.UnaryOp:
        return ast.UnaryOp(op=ast.Not(), operand=self.stack.pop())

    def UNARY_INVERT(self) -> ast.Invert:
        return ast.Invert(self.stack.pop())  # pyright: ignore[reportCallIssue]

    def UNPACK_SEQUENCE(self, count: int) -> ast.Assign:
        ass_tuple = ast.Assign(
            targets=cast("list[ast.expr]", ast.Tuple([], ast.Store())),
            value=ast.Constant(None),
        )
        ass_tuple.count = count  # pyright: ignore[reportAttributeAccessIssue]
        return ass_tuple

    def YIELD_VALUE(self, _: None = None) -> ast.GeneratorExp:
        expr = self.stack.pop()
        generators = []
        while self.stack:
            self.process_target(None)
            top = self.stack.pop()
            if not isinstance(top, ast.comprehension):
                cond = top
                top = self.stack.pop()
                assert isinstance(top, ast.comprehension)
                top.ifs.append(cond)
                generators.append(top)
            else:
                generators.append(top)
        generators.reverse()
        return ast.GeneratorExp(simplify(expr), generators)


test_lines: str = """
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
    (a for a in T if a.b is None)
    (a for a in T if a.b is not None)
    (a for a in T if a.b is None or a.b == c)
    (a for a in T if a.b is not None or a.b == c)
    (a for a in T if a.b is None and a.c == d)
    (a for a in T if a.b is not None and a.c == d)
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


def test(test_line: int | None = None) -> None:
    import sys

    if sys.version[:3] > "2.4":
        outmost_iterable_name = ".0"
    else:
        outmost_iterable_name = "[outmost-iterable]"
    import dis

    for i, line in enumerate(test_lines.split("\n")):
        if test_line is not None and i != test_line:
            continue
        if not line or line.isspace():
            continue
        line = line.strip()
        if line.startswith("#"):
            continue
        code = compile(line, "<?>", "eval").co_consts[0]
        ast1 = ast.parse(line).body[0]
        ast1.value.generators[0].iter.id = outmost_iterable_name  # pyright: ignore[reportAttributeAccessIssue]
        ast1 = ast.dump(ast1)
        try:
            ast2 = ast.Expr(Decompiler(code).ast)
            ast2 = ast.dump(ast2)
        except Exception:
            print()
            print(i, line)
            print()
            print(ast1)
            print()
            dis.dis(code)
            raise
        if ast1 != ast2:
            print()
            print(i, line)
            print()
            print(ast1)
            print()
            print(ast2)
            print()
            dis.dis(code)
            break
        else:
            print("%d OK: %s" % (i, line))
    else:
        print("Done!")


if __name__ == "__main__":
    test()
