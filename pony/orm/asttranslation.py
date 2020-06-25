from __future__ import absolute_import, print_function, division
from pony.py23compat import PY38

import ast

from functools import update_wrapper

from pony.utils import HashableDict, throw, copy_ast

class TranslationError(Exception): pass

pre_method_caches = {}
post_method_caches = {}

class ASTTranslator(object):
    def __init__(translator, tree):
        translator.tree = tree
        translator_cls = translator.__class__
        pre_method_caches.setdefault(translator_cls, {})
        post_method_caches.setdefault(translator_cls, {})
    def dispatch(translator, node):
        translator_cls = translator.__class__
        pre_methods = pre_method_caches[translator_cls]
        post_methods = post_method_caches[translator_cls]
        if isinstance(node, (ast.BoolOp, ast.BinOp, ast.UnaryOp)):
            node_cls = node.op.__class__
        else:
            node_cls = node.__class__

        try:
            pre_method = pre_methods[node_cls]
        except KeyError:
            pre_method = getattr(translator_cls, 'pre' + node_cls.__name__, translator_cls.default_pre)
            pre_methods[node_cls] = pre_method

        stop = translator.call(pre_method, node)
        if stop: return

        for child in get_child_nodes(node):
            translator.dispatch(child)

        try:
            post_method = post_methods[node_cls]
        except KeyError:
            post_method = getattr(translator_cls, 'post' + node_cls.__name__, translator_cls.default_post)
            post_methods[node_cls] = post_method
        translator.call(post_method, node)
    def call(translator, method, node):
        return method(translator, node)
    def default_pre(translator, node):
        pass
    def default_post(translator, node):
        pass

def priority(p):
    def decorator(func):
        def new_func(translator, node):
            node.priority = p
            for child in get_child_nodes(node):
                if getattr(child, 'priority', 0) >= p:
                    child.src = '(%s)' % child.src
            return func(translator, node)
        return update_wrapper(new_func, func)
    return decorator


def binop_src(op, node):
    return op.join((node.left.src, node.right.src))


def ast2src(tree):
    src = getattr(tree, 'src', None)
    if src is not None:
        return src
    PythonTranslator(tree)
    return tree.src


def get_child_nodes(node):
    for child in ast.iter_child_nodes(node):
        if not isinstance(child, (ast.expr_context, ast.boolop, ast.unaryop, ast.operator)):
            yield child


class PythonTranslator(ASTTranslator):
    def __init__(translator, tree):
        ASTTranslator.__init__(translator, tree)
        translator.top_level_f_str = None
        translator.dispatch(tree)
    def call(translator, method, node):
        node.src = method(translator, node)
    def default_pre(translator, node):
        if getattr(node, 'src', None) is not None:
            return True  # node.src is already calculated, stop dispatching
    def default_post(translator, node):
        throw(NotImplementedError, node)
    def postGeneratorExp(translator, node):
        return '(' + node.elt.src + ' ' + ' '.join(gen.src for gen in node.generators) + ')'
    def postcomprehension(translator, node):
        src = 'for %s in %s' % (node.target.src, node.iter.src)
        if node.ifs:
            ifs = ' '.join('if ' + if_.src for if_ in node.ifs)
            src += ' ' + ifs
        return src
    def postGenExprIf(translator, node):
        return 'if %s' % node.test.src
    def postExpr(translator, node):
        return node.value.src
    def postIfExp(translator, node):
        return '%s if %s else %s' % (node.body.src, node.test.src, node.orelse.src)
    def postLambda(translator, node):
        return 'lambda %s: %s' % (node.args.src, node.body.src)
    def postarguments(translator, node):
        if node.defaults:
            nodef_args = node.args[:-len(node.defaults)]
            def_args = node.args[-len(node.defaults):]
        else:
            nodef_args = node.args
            def_args = []

        result = [arg.arg for arg in nodef_args]
        result.extend('%s=%s' % (arg.arg, default.src) for arg, default in zip(def_args, node.defaults))
        if node.vararg:
            result.append('*%s' % node.vararg.arg)
        if node.kwarg:
            result.append('**%s' % node.kwarg.arg)
        return ', '.join(result)
    def postarg(translator, node):
        return node.arg
    @priority(14)
    def postOr(translator, node):
        return ' or '.join(expr.src for expr in node.values)
    @priority(13)
    def postAnd(translator, node):
        return ' and '.join(expr.src for expr in node.values)
    @priority(12)
    def postNot(translator, node):
        return 'not ' + node.operand.src
    @priority(11)
    def postCompare(translator, node):
        result = [ node.left.src ]
        for op, expr in zip(node.ops, node.comparators):
            result.extend((op.src, expr.src))
        return ' '.join(result)
    def postEq(translator, node):
        return '=='
    def postNotEq(translator, node):
        return '!='
    def postLt(translator, node):
        return '<'
    def postLtE(translator, node):
        return '<='
    def postGt(translator, node):
        return '>'
    def postGtE(translator, node):
        return '>='
    def postIs(translator, node):
        return 'is'
    def postIsNot(translator, node):
        return 'is not'
    def postIn(translator, node):
        return 'in'
    def postNotIn(translator, node):
        return 'not in'
    @priority(10)
    def postBitOr(translator, node):
        return ' | '.join((node.left.src, node.right.src))
    @priority(9)
    def postBitXor(translator, node):
        return ' ^ '.join((node.left.src, node.right.src))
    @priority(8)
    def postBitAnd(translator, node):
        return ' & '.join((node.left.src, node.right.src))
    @priority(7)
    def postLShift(translator, node):
        return ' << '.join((node.left.src, node.right.src))
    @priority(7)
    def postRShift(translator, node):
        return ' >> '.join((node.left.src, node.right.src))
    @priority(6)
    def postAdd(translator, node):
        return ' + '.join((node.left.src, node.right.src))
    @priority(6)
    def postSub(translator, node):
        return ' - '.join((node.left.src, node.right.src))
    @priority(5)
    def postMult(translator, node):
        return ' * '.join((node.left.src, node.right.src))
    @priority(5)
    def postMatMult(translator, node):
        throw(NotImplementedError)
    @priority(5)
    def postDiv(translator, node):
        return ' / '.join((node.left.src, node.right.src))
    @priority(5)
    def postFloorDiv(translator, node):
        return ' // '.join((node.left.src, node.right.src))
    @priority(5)
    def postMod(translator, node):
        return ' % '.join((node.left.src, node.right.src))
    @priority(4)
    def postUSub(translator, node):
        return '-' + node.operand.src
    @priority(4)
    def postUAdd(translator, node):
        return '+' + node.operand.src
    @priority(4)
    def postInvert(translator, node):
        return '~' + node.expr.src
    @priority(3)
    def postPow(translator, node):
        return binop_src(' ** ', node)
    def postAttribute(translator, node):
        node.priority = 2
        return '.'.join((node.value.src, node.attr))
    def postCall(translator, node):
        node.priority = 2
        if len(node.args) == 1 and isinstance(node.args[0], ast.GeneratorExp):
            return node.func.src + node.args[0].src
        args = [ arg.src for arg in node.args ] + [ kw.src for kw in node.keywords ]
        return '%s(%s)' % (node.func.src, ', '.join(args))
    def postkeyword(translator, node):
        if node.arg is None:
            return '**' + node.value.src
        return '%s=%s' % (node.arg, node.value.src)
    def postStarred(translator, node):
        return '*' + node.value.src
    def postSubscript(translator, node):
        node.priority = 2
        x = node.slice
        if isinstance(x, ast.Index):
            x = x.value
        if isinstance(x, ast.Tuple):
            key = ', '.join([elt.src for elt in x.elts])
        elif isinstance(x, ast.Constant) and isinstance(x.value, tuple):
            key = repr(x.value)[1:-1]
        else:
            key = x.src
        return '%s[%s]' % (node.value.src, key)
    def postIndex(translator, node):  # Python <= 3.7
        return node.value.src
    def postSlice(translator, node):
        result = []
        if node.lower:
            result.append(node.lower.src)
        result.append(':')
        if node.upper:
            result.append(node.upper.src)
        if node.step:
            result.append(':')
            result.append(node.step.src)
        return ''.join(result)
    def postConstant(translator, node):
        node.priority = 1
        value = node.value
        if type(value) is float: # for Python < 2.7
            s = str(value)
            if float(s) == value: return s
        return repr(value)
    def postNameConstant(translator, node):  # Python <= 3.7
        return repr(node.value)
    def postNum(translator, node):  # Python <= 3.7
        node.priority = 1
        return repr(node.n)
    def postStr(translator, node):  # Python <= 3.7
        node.priority = 1
        return repr(node.s)
    def postBytes(translator, node):  # Python <= 3.7
        node.priority = 1
        return repr(node.s)
    def postList(translator, node):
        node.priority = 1
        return '[%s]' % ', '.join(item.src for item in node.elts)
    def postTuple(translator, node):
        node.priority = 1
        if len(node.elts) == 1:
            return '(%s,)' % node.elts[0].src
        return '(%s)' % ', '.join(elt.src for elt in node.elts)
    def postDict(translator, node):
        node.priority = 1
        return '{%s}' % ', '.join('%s:%s' % (key.src, value.src) for key, value in zip(node.keys, node.values))
    def postSet(translator, node):
        node.priority = 1
        return '{%s}' % ', '.join(item.src for item in node.nodes)
    def postName(translator, node):
        node.priority = 1
        return node.id
    def postJoinedStr(self, node):
        result = []
        for item in node.values:
            if isinstance(item, ast.Constant):
                assert isinstance(item.value, str)
                result.append(item.value)
            elif not PY38 and isinstance(item, ast.Str):  # Python 3.7
                result.append(item.s)
            elif isinstance(item, ast.FormattedValue):
                if item.conversion == -1:
                    src = '{%s}' % item.value.src
                else:
                    src = '{%s!%s}' % (item.value.src, chr(item.conversion))
                result.append(src)
            else:
                assert False
        return "f%r" % ''.join(result)
    def postFormattedValue(self, node):
        return node.value.src


nonexternalizable_types = (ast.keyword, ast.Starred, ast.Slice, ast.List, ast.Tuple)


class PreTranslator(ASTTranslator):
    def __init__(translator, tree, globals, locals,
                 special_functions, const_functions, outer_names=()):
        ASTTranslator.__init__(translator, tree)
        translator.globals = globals
        translator.locals = locals
        translator.special_functions = special_functions
        translator.const_functions = const_functions
        translator.contexts = []
        if outer_names:
            translator.contexts.append(outer_names)
        translator.externals = externals = set()
        translator.dispatch(tree)
        for node in externals.copy():
            if isinstance(node, nonexternalizable_types) or node.constant and not isinstance(node, ast.Constant):
                node.external = False
                externals.remove(node)
                externals.update(node for node in get_child_nodes(node) if node.external and not node.constant)
    def dispatch(translator, node):
        node.external = node.constant = None
        ASTTranslator.dispatch(translator, node)
        children = list(get_child_nodes(node))
        if node.external is None and children and all(
                getattr(child, 'external', False) and not getattr(child, 'raw_sql', False) for child in children):
            node.external = True
        if node.external and not node.constant:
            externals = translator.externals
            externals.difference_update(children)
            externals.add(node)
    def preGeneratorExp(translator, node):
        translator.contexts.append(set())
        dispatch = translator.dispatch
        for i, qual in enumerate(node.generators):
            dispatch(qual.iter)
            dispatch(qual.target)
            for if_ in qual.ifs:
                dispatch(if_)
        dispatch(node.elt)
        translator.contexts.pop()
        return True
    def preLambda(translator, node):
        #if node.varargs or node.kwargs or node.defaults:
        #    throw(NotImplementedError)
        context = set(arg.arg for arg in node.args.args)
        if node.args.vararg:
            context.add(node.args.vararg.arg)
        if node.args.kwarg:
            context.add(node.args.kwarg.arg)
        translator.contexts.append(context)
        translator.dispatch(node.body)
        translator.contexts.pop()
        return True
    def postName(translator, node):
        name = node.id
        if isinstance(node.ctx, ast.Store):
            if name.startswith('__'):
                throw(TranslationError, 'Illegal name: %r' % name)
            translator.contexts[-1].add(name)
            return
        elif isinstance(node.ctx, ast.Load):
            for context in translator.contexts:
                if name in context:
                    return
            node.external = True
        else:
            assert False, type(node.ctx)
    def postSlice(translator, node):
        if node.lower is None and node.upper is None and node.step is None:
            node.external = node.constant = True
    def postStarred(translator, node):
        node.external = True
    def postConstant(translator, node):
        node.external = node.constant = True
    def postNum(translator, node):  # Python <= 3.7
        node.external = node.constant = True
    def postStr(translator, node):  # Python <= 3.7
        node.external = node.constant = True
    def postBytes(translator, node):  # Python <= 3.7
        node.external = node.constant = True
    def postDict(translator, node):
        node.external = True
    def postList(translator, node):
        node.external = True
    def postkeyword(translator, node):
        node.constant = node.value.constant
    def postIndex(translator, node):  # Python <= 3.7
        node.constant = node.value.constant
    def postCall(translator, node):
        func_node = node.func
        if not func_node.external:
            return
        attrs = []
        while isinstance(func_node, ast.Attribute):
            attrs.append(func_node.attr)
            func_node = func_node.value
        if not isinstance(func_node, ast.Name):
            return
        attrs.append(func_node.id)
        expr = '.'.join(reversed(attrs))
        x = eval(expr, translator.globals, translator.locals)
        try:
            hash(x)
        except TypeError:
            return

        if x in translator.special_functions:
            if x.__name__ == 'raw_sql':
                node.raw_sql = True
            elif x is getattr:
                attr_node = node.args[1]
                attr_node.parent_node = node
            else:
                node.external = False
        elif x in translator.const_functions:
            for arg in node.args:
                if not arg.constant:
                    return
            if any(not arg.constant for arg in node.args if isinstance(arg, ast.Starred)):
                return
            if any(not kwarg.constant for kwarg in node.keywords if kwarg.arg is None):
                return
            node.constant = True
    def postCompare(translator, node):
        for op in node.ops:
            op.external = op.constant = True
    def post_binop(translator, node):
        pass

    postBitOr = postBitXor = postBitAnd = postLShift = postRShift \
        = postAdd = postSub = postMult = postMatMult = postDiv = postFloorDiv = postMod = post_binop

extractors_cache = {}

def create_extractors(code_key, tree, globals, locals, special_functions, const_functions, outer_names=()):
    result = extractors_cache.get(code_key)
    if not result:
        pretranslator = PreTranslator(tree, globals, locals, special_functions, const_functions, outer_names)
        extractors = {}
        for node in pretranslator.externals:
            src = node.src = ast2src(node)
            if src == '.0':
                def extractor(globals, locals):
                    return locals['.0']
            else:
                filename = '<pony ' + src + '>'
                code = compile(src, filename, 'eval')
                def extractor(globals, locals, code=code):
                    return eval(code, globals, locals)
            extractors[src] = extractor
        result = tree, extractors
    return result
