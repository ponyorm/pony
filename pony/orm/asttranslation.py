from __future__ import annotations

import ast
from functools import update_wrapper
from typing import TYPE_CHECKING, Any, Generic, NoReturn, TypeVar, cast

from pony.utils import throw

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

    from pony.orm.core import CodeKey, ConstType

_ExprT = TypeVar("_ExprT", bound=ast.expr)
_TreeT = TypeVar("_TreeT", bound=ast.AST)
_VisitorT = TypeVar("_VisitorT", bound="Callable[..., Any]")


class TranslationError(Exception):
    pass


# Per translator class: node class -> the pre*/post* method that handles it
pre_method_caches: dict[
    type[ASTTranslator[Any]], dict[type[ast.AST], Callable[[Any, ast.AST], Any]]
] = {}
post_method_caches: dict[
    type[ASTTranslator[Any]], dict[type[ast.AST], Callable[[Any, ast.AST], Any]]
] = {}


class ASTTranslator(Generic[_TreeT]):
    tree: _TreeT

    def __init__(self, tree: _TreeT) -> None:
        self.tree = tree
        translator_cls = self.__class__
        pre_method_caches.setdefault(translator_cls, {})
        post_method_caches.setdefault(translator_cls, {})

    def dispatch(self, node: ast.AST) -> None:
        translator_cls = self.__class__
        pre_methods = pre_method_caches[translator_cls]
        post_methods = post_method_caches[translator_cls]
        node_cls: type[ast.AST]
        if isinstance(node, (ast.BoolOp, ast.BinOp, ast.UnaryOp)):
            node_cls = node.op.__class__
        else:
            node_cls = node.__class__

        try:
            pre_method = pre_methods[node_cls]
        except KeyError:
            pre_method = getattr(
                translator_cls, "pre" + node_cls.__name__, translator_cls.default_pre
            )
            pre_methods[node_cls] = pre_method

        stop = self.call(pre_method, node)
        if stop:
            return

        for child in get_child_nodes(node):
            self.dispatch(child)

        try:
            post_method = post_methods[node_cls]
        except KeyError:
            post_method = getattr(
                translator_cls, "post" + node_cls.__name__, translator_cls.default_post
            )
            post_methods[node_cls] = post_method
        self.call(post_method, node)

    def call(self, method: Callable[[Any, ast.AST], Any], node: ast.AST) -> Any:
        return method(self, node)

    def default_pre(self, node: ast.AST) -> bool | None:
        pass

    def default_post(self, node: ast.AST) -> None:
        pass


def priority(p: int) -> Callable[[_VisitorT], _VisitorT]:
    def decorator(func: _VisitorT) -> _VisitorT:
        def new_func(self: Any, node: Any) -> Any:
            node.priority = p
            for child in get_child_nodes(node):
                if getattr(child, "priority", 0) >= p:
                    child.src = "(%s)" % child.src  # type: ignore[attr-defined]
            return func(self, node)

        return cast("_VisitorT", update_wrapper(new_func, func))

    return decorator


def binop_src(op: str, node: ast.BinOp) -> str:
    return op.join((node.left.src, node.right.src))  # type: ignore[attr-defined]


def unaryop_src(op: str, node: ast.UnaryOp) -> str:
    return op + node.operand.src  # type: ignore[attr-defined]


def ast2src(tree: ast.AST) -> str:
    src = getattr(tree, "src", None)
    if src is not None:
        return src
    PythonTranslator(tree)
    return tree.src  # type: ignore[attr-defined]


def get_child_nodes(node: ast.AST) -> Iterator[ast.AST]:
    for child in ast.iter_child_nodes(node):
        if not isinstance(
            child, (ast.expr_context, ast.boolop, ast.unaryop, ast.operator)
        ):
            yield child


class PythonTranslator(ASTTranslator[ast.AST]):
    top_level_f_str: str | None

    def __init__(self, tree: ast.AST) -> None:
        ASTTranslator.__init__(self, tree)
        self.top_level_f_str = None
        self.dispatch(tree)

    def call(self, method: Callable[[Any, ast.AST], Any], node: ast.AST) -> None:
        node.src = method(self, node)  # type: ignore[attr-defined]

    def default_pre(self, node: ast.AST) -> bool | None:
        if getattr(node, "src", None) is not None:
            return True  # node.src is already calculated, stop dispatching
        return None

    def default_post(self, node: ast.AST) -> NoReturn:
        throw(NotImplementedError, node)

    def postGeneratorExp(self, node: ast.GeneratorExp) -> str:
        return (
            "("
            + node.elt.src  # type: ignore[attr-defined]
            + " "
            + " ".join(gen.src for gen in node.generators)  # type: ignore[attr-defined]
            + ")"
        )

    def postcomprehension(self, node: ast.comprehension) -> str:
        src = "for %s in %s" % (node.target.src, node.iter.src)  # type: ignore[attr-defined]
        if node.ifs:
            ifs = " ".join("if " + if_.src for if_ in node.ifs)  # type: ignore[attr-defined]
            src += " " + ifs
        return src

    def postExpr(self, node: ast.Expr) -> str:
        return node.value.src  # type: ignore[attr-defined]

    def postIfExp(self, node: ast.IfExp) -> str:
        return "%s if %s else %s" % (node.body.src, node.test.src, node.orelse.src)  # type: ignore[attr-defined]

    def postLambda(self, node: ast.Lambda) -> str:
        return "lambda %s: %s" % (node.args.src, node.body.src)  # type: ignore[attr-defined]

    def postarguments(self, node: ast.arguments) -> str:
        if node.defaults:
            nodef_args = node.args[: -len(node.defaults)]
            def_args = node.args[-len(node.defaults) :]
        else:
            nodef_args = node.args
            def_args = []

        result = [arg.arg for arg in nodef_args]
        result.extend(
            "%s=%s" % (arg.arg, default.src)  # type: ignore[attr-defined]
            for arg, default in zip(def_args, node.defaults)
        )
        if node.vararg:
            result.append("*%s" % node.vararg.arg)
        if node.kwarg:
            result.append("**%s" % node.kwarg.arg)
        return ", ".join(result)

    def postarg(self, node: ast.arg) -> str:
        return node.arg

    @priority(14)
    def postOr(self, node: ast.BoolOp) -> str:
        return " or ".join(expr.src for expr in node.values)  # type: ignore[attr-defined]

    @priority(13)
    def postAnd(self, node: ast.BoolOp) -> str:
        return " and ".join(expr.src for expr in node.values)  # type: ignore[attr-defined]

    @priority(12)
    def postNot(self, node: ast.UnaryOp) -> str:
        return unaryop_src("not ", node)

    @priority(11)
    def postCompare(self, node: ast.Compare) -> str:
        result = [node.left.src]  # type: ignore[attr-defined]
        for op, expr in zip(node.ops, node.comparators):
            result.extend((op.src, expr.src))  # type: ignore[attr-defined]
        return " ".join(result)

    def postEq(self, node: ast.Eq) -> str:
        return "=="

    def postNotEq(self, node: ast.NotEq) -> str:
        return "!="

    def postLt(self, node: ast.Lt) -> str:
        return "<"

    def postLtE(self, node: ast.LtE) -> str:
        return "<="

    def postGt(self, node: ast.Gt) -> str:
        return ">"

    def postGtE(self, node: ast.GtE) -> str:
        return ">="

    def postIs(self, node: ast.Is) -> str:
        return "is"

    def postIsNot(self, node: ast.IsNot) -> str:
        return "is not"

    def postIn(self, node: ast.In) -> str:
        return "in"

    def postNotIn(self, node: ast.NotIn) -> str:
        return "not in"

    @priority(10)
    def postBitOr(self, node: ast.BinOp) -> str:
        return binop_src(" | ", node)

    @priority(9)
    def postBitXor(self, node: ast.BinOp) -> str:
        return binop_src(" ^ ", node)

    @priority(8)
    def postBitAnd(self, node: ast.BinOp) -> str:
        return binop_src(" & ", node)

    @priority(7)
    def postLShift(self, node: ast.BinOp) -> str:
        return binop_src(" << ", node)

    @priority(7)
    def postRShift(self, node: ast.BinOp) -> str:
        return binop_src(" >> ", node)

    @priority(6)
    def postAdd(self, node: ast.BinOp) -> str:
        return binop_src(" + ", node)

    @priority(6)
    def postSub(self, node: ast.BinOp) -> str:
        return binop_src(" - ", node)

    @priority(5)
    def postMult(self, node: ast.BinOp) -> str:
        return binop_src(" * ", node)

    @priority(5)
    def postMatMult(self, node: ast.BinOp) -> NoReturn:
        throw(NotImplementedError)

    @priority(5)
    def postDiv(self, node: ast.BinOp) -> str:
        return binop_src(" / ", node)

    @priority(5)
    def postFloorDiv(self, node: ast.BinOp) -> str:
        return binop_src(" // ", node)

    @priority(5)
    def postMod(self, node: ast.BinOp) -> str:
        return binop_src(" % ", node)

    @priority(4)
    def postUSub(self, node: ast.UnaryOp) -> str:
        return unaryop_src("-", node)

    @priority(4)
    def postUAdd(self, node: ast.UnaryOp) -> str:
        return unaryop_src("+", node)

    @priority(4)
    def postInvert(self, node: ast.UnaryOp) -> str:
        return "~" + node.operand.src  # type: ignore[attr-defined]

    @priority(3)
    def postPow(self, node: ast.BinOp) -> str:
        return binop_src(" ** ", node)

    def postAttribute(self, node: ast.Attribute) -> str:
        node.priority = 2  # type: ignore[attr-defined]
        return ".".join((node.value.src, node.attr))  # type: ignore[attr-defined]

    def postCall(self, node: ast.Call) -> str:
        node.priority = 2  # type: ignore[attr-defined]
        if len(node.args) == 1 and isinstance(node.args[0], ast.GeneratorExp):
            return node.func.src + node.args[0].src  # type: ignore[attr-defined]
        args = [arg.src for arg in node.args] + [kw.src for kw in node.keywords]  # type: ignore[attr-defined]
        return "%s(%s)" % (node.func.src, ", ".join(args))  # type: ignore[attr-defined]

    def postkeyword(self, node: ast.keyword) -> str:
        if node.arg is None:
            return "**" + node.value.src  # type: ignore[attr-defined]
        return "%s=%s" % (node.arg, node.value.src)  # type: ignore[attr-defined]

    def postStarred(self, node: ast.Starred) -> str:
        return "*" + node.value.src  # type: ignore[attr-defined]

    def postSubscript(self, node: ast.Subscript) -> str:
        node.priority = 2  # type: ignore[attr-defined]
        x = node.slice
        if isinstance(x, ast.Tuple):
            key = ", ".join([elt.src for elt in x.elts])  # type: ignore[attr-defined]
        elif isinstance(x, ast.Constant) and isinstance(x.value, tuple):
            key = repr(x.value)[1:-1]
        else:
            key = x.src  # type: ignore[attr-defined]
        return "%s[%s]" % (node.value.src, key)  # type: ignore[attr-defined]

    def postSlice(self, node: ast.Slice) -> str:
        result = []
        if node.lower:
            result.append(node.lower.src)  # type: ignore[attr-defined]
        result.append(":")
        if node.upper:
            result.append(node.upper.src)  # type: ignore[attr-defined]
        if node.step:
            result.append(":")
            result.append(node.step.src)  # type: ignore[attr-defined]
        return "".join(result)

    def postConstant(self, node: ast.Constant) -> str:
        node.priority = 1  # type: ignore[attr-defined]
        value = node.value
        if type(value) is float:  # for Python < 2.7
            s = str(value)
            if float(s) == value:
                return s
        return repr(value)

    def postList(self, node: ast.List) -> str:
        node.priority = 1  # type: ignore[attr-defined]
        return "[%s]" % ", ".join(item.src for item in node.elts)  # type: ignore[attr-defined]

    def postTuple(self, node: ast.Tuple) -> str:
        node.priority = 1  # type: ignore[attr-defined]
        if len(node.elts) == 1:
            return "(%s,)" % node.elts[0].src  # type: ignore[attr-defined]
        return "(%s)" % ", ".join(elt.src for elt in node.elts)  # type: ignore[attr-defined]

    def postDict(self, node: ast.Dict) -> str:
        node.priority = 1  # type: ignore[attr-defined]
        return "{%s}" % ", ".join(
            "%s:%s" % (key.src, value.src)  # type: ignore[attr-defined]
            for key, value in zip(cast("list[ast.expr]", node.keys), node.values)
        )

    def postSet(self, node):
        node.priority = 1
        return "{%s}" % ", ".join(item.src for item in node.elts)

    def postName(self, node: ast.Name) -> str:
        node.priority = 1  # type: ignore[attr-defined]
        return node.id

    def postJoinedStr(self, node: ast.JoinedStr) -> str:
        result = []
        for item in node.values:
            if isinstance(item, ast.Constant):
                assert isinstance(item.value, str)
                result.append(item.value)
            elif isinstance(item, ast.FormattedValue):
                if item.conversion == -1:
                    src = "{%s}" % item.value.src  # type: ignore[attr-defined]
                else:
                    src = "{%s!%s}" % (item.value.src, chr(item.conversion))  # type: ignore[attr-defined]
                result.append(src)
            else:
                assert False
        return "f%r" % "".join(result)

    def postFormattedValue(self, node: ast.FormattedValue) -> str:
        return node.value.src  # type: ignore[attr-defined]


nonexternalizable_types = (ast.keyword, ast.Starred, ast.Slice, ast.List, ast.Tuple)


class PreTranslator(ASTTranslator[ast.AST]):
    globals: dict[str, Any]
    locals: Mapping[str, object]
    special_functions: set[Callable]
    const_functions: set[ConstType]
    contexts: list[Iterable[str]]
    externals: set[ast.AST]

    def __init__(
        self,
        tree: ast.AST,
        globals: dict[str, Any],
        locals: Mapping[str, object],
        special_functions: set[Callable],
        const_functions: set[ConstType],
        outer_names: Sequence[str] = (),
    ) -> None:
        ASTTranslator.__init__(self, tree)
        self.globals = globals
        self.locals = locals
        self.special_functions = special_functions
        self.const_functions = const_functions
        self.contexts = []
        if outer_names:
            self.contexts.append(outer_names)
        externals: set[ast.AST] = set()
        self.externals = externals
        self.dispatch(tree)
        for node in externals.copy():
            if (
                isinstance(node, nonexternalizable_types)
                or node.constant  # type: ignore[attr-defined]
                and not isinstance(node, ast.Constant)
            ):
                node.external = False  # type: ignore[attr-defined]
                externals.remove(node)
                externals.update(
                    node
                    for node in get_child_nodes(node)
                    if node.external and not node.constant  # type: ignore[attr-defined]
                )

    def dispatch(self, node: ast.AST) -> None:
        node.external = node.constant = None  # type: ignore[attr-defined]
        ASTTranslator.dispatch(self, node)
        children = list(get_child_nodes(node))
        if (
            node.external is None  # type: ignore[attr-defined]
            and children
            and all(
                getattr(child, "external", False)
                and not getattr(child, "raw_sql", False)
                for child in children
            )
        ):
            node.external = True  # type: ignore[attr-defined]
        if node.external and not node.constant:  # type: ignore[attr-defined]
            externals = self.externals
            externals.difference_update(children)
            externals.add(node)

    def preGeneratorExp(self, node: ast.GeneratorExp) -> bool:
        self.contexts.append(set())
        dispatch = self.dispatch
        for _i, qual in enumerate(node.generators):
            dispatch(qual.iter)
            dispatch(qual.target)
            for if_ in qual.ifs:
                dispatch(if_)
        dispatch(node.elt)
        self.contexts.pop()
        return True

    def preLambda(self, node: ast.Lambda) -> bool:
        # if node.varargs or node.kwargs or node.defaults:
        #    throw(NotImplementedError)
        context = set(arg.arg for arg in node.args.args)
        if node.args.vararg:
            context.add(node.args.vararg.arg)
        if node.args.kwarg:
            context.add(node.args.kwarg.arg)
        self.contexts.append(context)
        self.dispatch(node.body)
        self.contexts.pop()
        return True

    def postName(self, node: ast.Name) -> None:
        name = node.id
        if isinstance(node.ctx, ast.Store):
            if name.startswith("__"):
                throw(TranslationError, "Illegal name: %r" % name)
            assert isinstance(self.contexts[-1], set)
            self.contexts[-1].add(name)
            return
        elif isinstance(node.ctx, ast.Load):
            for context in self.contexts:
                if name in context:
                    return
            node.external = True  # type: ignore[attr-defined]
        else:
            assert False, type(node.ctx)

    def postSlice(self, node: ast.Slice) -> None:
        if node.lower is None and node.upper is None and node.step is None:
            node.external = node.constant = True  # type: ignore[attr-defined]

    def postStarred(self, node: ast.Starred) -> None:
        node.external = True  # type: ignore[attr-defined]

    def postConstant(self, node: ast.Constant) -> None:
        node.external = node.constant = True  # type: ignore[attr-defined]

    def postDict(self, node: ast.Dict) -> None:
        node.external = True  # type: ignore[attr-defined]

    def postList(self, node: ast.List) -> None:
        node.external = True  # type: ignore[attr-defined]

    def postkeyword(self, node: ast.keyword) -> None:
        node.constant = node.value.constant  # type: ignore[attr-defined]

    def postCall(self, node: ast.Call) -> None:
        func_node = node.func
        if not func_node.external:  # type: ignore[attr-defined]
            return
        attrs = []
        while isinstance(func_node, ast.Attribute):
            attrs.append(func_node.attr)
            func_node = func_node.value
        if not isinstance(func_node, ast.Name):
            return
        attrs.append(func_node.id)
        expr = ".".join(reversed(attrs))
        x = eval(expr, self.globals, self.locals)
        try:
            hash(x)
        except TypeError:
            return

        if x in self.special_functions:
            if x.__name__ == "raw_sql":
                node.raw_sql = True  # type: ignore[attr-defined]
            elif x is getattr:
                attr_node = node.args[1]
                attr_node.parent_node = node  # type: ignore[attr-defined]
            else:
                node.external = False  # type: ignore[attr-defined]
        elif x in self.const_functions:
            for arg in node.args:
                if not arg.constant:  # type: ignore[attr-defined]
                    return
            if any(
                not arg.constant  # type: ignore[attr-defined]
                for arg in node.args
                if isinstance(arg, ast.Starred)
            ):
                return
            if any(not kwarg.constant for kwarg in node.keywords if kwarg.arg is None):  # type: ignore[attr-defined]
                return
            node.constant = True  # type: ignore[attr-defined]

    def postCompare(self, node: ast.Compare) -> None:
        for op in node.ops:
            op.external = op.constant = True  # type: ignore[attr-defined]

    def post_binop(self, node: ast.BinOp) -> None:
        pass

    postBitOr = postBitXor = postBitAnd = postLShift = postRShift = postAdd = (
        postSub
    ) = postMult = postMatMult = postDiv = postFloorDiv = postMod = post_binop


extractors_cache: dict[CodeKey, tuple[Any, dict[str, Callable]]] = {}


def create_extractors(
    code_key: CodeKey,
    tree: _ExprT,
    globals: dict[str, Any],
    locals: Mapping[str, object],
    special_functions: set[Callable],
    const_functions: set[ConstType],
    outer_names: Sequence[str] = (),
) -> tuple[_ExprT, dict[str, Callable]]:
    result = extractors_cache.get(code_key)
    if not result:
        pretranslator = PreTranslator(
            tree, globals, locals, special_functions, const_functions, outer_names
        )
        extractors = {}
        for node in pretranslator.externals:
            src = node.src = ast2src(node)  # type: ignore[attr-defined]
            if src == ".0":

                def dot_zero_extractor(globals, locals):
                    return locals[".0"]

                extractor = dot_zero_extractor
            else:
                filename = "<pony " + src + ">"
                code = compile(src, filename, "eval")

                def eval_extractor(globals, locals, code=code):
                    return eval(code, globals, locals)

                extractor = eval_extractor
            extractors[src] = extractor
        result = extractors_cache[code_key] = tree, extractors
    return result
