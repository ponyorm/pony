from __future__ import absolute_import, print_function, division
from pony.py23compat import basestring

from functools import update_wrapper

from pony.thirdparty.compiler import ast

from pony.utils import throw

class TranslationError(Exception): pass

class ASTTranslator(object):
    def __init__(translator, tree):
        translator.tree = tree
        translator.pre_methods = {}
        translator.post_methods = {}
    def dispatch(translator, node):
        cls = node.__class__

        try: pre_method = translator.pre_methods[cls]
        except KeyError:
            pre_method = getattr(translator, 'pre' + cls.__name__, translator.default_pre)
            translator.pre_methods[cls] = pre_method
        stop = translator.call(pre_method, node)

        if stop: return

        for child in node.getChildNodes():
            translator.dispatch(child)

        try: post_method = translator.post_methods[cls]
        except KeyError:
            post_method = getattr(translator, 'post' + cls.__name__, translator.default_post)
            translator.post_methods[cls] = post_method
        translator.call(post_method, node)
    def call(translator, method, node):
        return method(node)
    def default_pre(translator, node):
        pass
    def default_post(translator, node):
        pass

def priority(p):
    def decorator(func):
        def new_func(translator, node):
            node.priority = p
            for child in node.getChildNodes():
                if getattr(child, 'priority', 0) >= p: child.src = '(%s)' % child.src
            return func(translator, node)
        return update_wrapper(new_func, func)
    return decorator

def binop_src(op, node):
    return op.join((node.left.src, node.right.src))

def ast2src(tree):
    PythonTranslator(tree)
    return tree.src

class PythonTranslator(ASTTranslator):
    def __init__(translator, tree):
        ASTTranslator.__init__(translator, tree)
        translator.dispatch(tree)
    def call(translator, method, node):
        node.src = method(node)
    def default_post(translator, node):
        throw(NotImplementedError, node)
    def postGenExpr(translator, node):
        return '(%s)' % node.code.src
    def postGenExprInner(translator, node):
        return node.expr.src + ' ' + ' '.join(qual.src for qual in node.quals)
    def postGenExprFor(translator, node):
        src = 'for %s in %s' % (node.assign.src, node.iter.src)
        if node.ifs:
            ifs = ' '.join(if_.src for if_ in node.ifs)
            src += ' ' + ifs
        return src
    def postGenExprIf(translator, node):
        return 'if %s' % node.test.src
    @priority(14)
    def postOr(translator, node):
        return ' or '.join(expr.src for expr in node.nodes)
    @priority(13)
    def postAnd(translator, node):
        return ' and '.join(expr.src for expr in node.nodes)
    @priority(12)
    def postNot(translator, node):
        return 'not ' + node.expr.src
    @priority(11)
    def postCompare(translator, node):
        result = [ node.expr.src ]
        for op, expr in node.ops: result.extend((op, expr.src))
        return ' '.join(result)
    @priority(10)
    def postBitor(translator, node):
        return ' | '.join(expr.src for expr in node.nodes)
    @priority(9)
    def postBitxor(translator, node):
        return ' ^ '.join(expr.src for expr in node.nodes)
    @priority(8)
    def postBitand(translator, node):
        return ' & '.join(expr.src for expr in node.nodes)
    @priority(7)
    def postLeftShift(translator, node):
        return binop_src(' << ', node)
    @priority(7)
    def postRightShift(translator, node):
        return binop_src(' >> ', node)
    @priority(6)
    def postAdd(translator, node):
        return binop_src(' + ', node)
    @priority(6)
    def postSub(translator, node):
        return binop_src(' - ', node)
    @priority(5)
    def postMul(translator, node):
        return binop_src(' * ', node)
    @priority(5)
    def postDiv(translator, node):
        return binop_src(' / ', node)
    @priority(5)
    def postFloorDiv(translator, node):
        return binop_src(' // ', node)
    @priority(5)
    def postMod(translator, node):
        return binop_src(' % ', node)
    @priority(4)
    def postUnarySub(translator, node):
        return '-' + node.expr.src
    @priority(4)
    def postUnaryAdd(translator, node):
        return '+' + node.expr.src
    @priority(4)
    def postInvert(translator, node):
        return '~' + node.expr.src
    @priority(3)
    def postPower(translator, node):
        return binop_src(' ** ', node)
    def postGetattr(translator, node):
        node.priority = 2
        return '.'.join((node.expr.src, node.attrname))
    def postCallFunc(translator, node):
        node.priority = 2
        args = [ arg.src for arg in node.args ]
        if node.star_args: args.append('*'+node.star_args.src)
        if node.dstar_args: args.append('**'+node.dstar_args.src)
        if len(args) == 1 and isinstance(node.args[0], ast.GenExpr):
            return node.node.src + args[0]
        return '%s(%s)' % (node.node.src, ', '.join(args))
    def postSubscript(translator, node):
        node.priority = 2
        if len(node.subs) == 1:
            sub = node.subs[0]
            if isinstance(sub, ast.Const) and type(sub.value) is tuple and len(sub.value) > 1:
                key = sub.src
                assert key.startswith('(') and key.endswith(')')
                key = key[1:-1]
            else: key = sub.src
        else: key = ', '.join([ sub.src for sub in node.subs ])
        return '%s[%s]' % (node.expr.src, key)
    def postSlice(translator, node):
        node.priority = 2
        lower = node.lower.src if node.lower is not None else ''
        upper = node.upper.src if node.upper is not None else ''
        return '%s[%s:%s]' % (node.expr.src, lower, upper)
    def postSliceobj(translator, node):
        return ':'.join(item.src for item in node.nodes)
    def postConst(translator, node):
        node.priority = 1
        value = node.value
        if type(value) is float: # for Python < 2.7
            s = str(value)
            if float(s) == value: return s
        return repr(value)
    def postEllipsis(translator, node):
        return '...'
    def postList(translator, node):
        node.priority = 1
        return '[%s]' % ', '.join(item.src for item in node.nodes)
    def postTuple(translator, node):
        node.priority = 1
        if len(node.nodes) == 1: return '(%s,)' % node.nodes[0].src
        else: return '(%s)' % ', '.join(item.src for item in node.nodes)
    def postAssTuple(translator, node):
        node.priority = 1
        if len(node.nodes) == 1: return '(%s,)' % node.nodes[0].src
        else: return '(%s)' % ', '.join(item.src for item in node.nodes)
    def postDict(translator, node):
        node.priority = 1
        return '{%s}' % ', '.join('%s:%s' % (key.src, value.src) for key, value in node.items)
    def postSet(translator, node):
        node.priority = 1
        return '{%s}' % ', '.join(item.src for item in node.nodes)
    def postBackquote(translator, node):
        node.priority = 1
        return '`%s`' % node.expr.src
    def postName(translator, node):
        node.priority = 1
        return node.name
    def postAssName(translator, node):
        node.priority = 1
        return node.name
    def postKeyword(translator, node):
        return '='.join((node.name, node.expr.src))

nonexternalizable_types = (ast.Keyword, ast.Sliceobj, ast.List, ast.Tuple)

class PreTranslator(ASTTranslator):
    def __init__(translator, tree, globals, locals,
                 special_functions, const_functions, additional_internal_names=()):
        ASTTranslator.__init__(translator, tree)
        translator.getattr_nodes = set()
        translator.globals = globals
        translator.locals = locals
        translator.special_functions = special_functions
        translator.const_functions = const_functions
        translator.contexts = []
        if additional_internal_names:
            translator.contexts.append(additional_internal_names)
        translator.externals = externals = set()
        translator.dispatch(tree)
        for node in externals.copy():
            if isinstance(node, nonexternalizable_types) \
            or node.constant and not isinstance(node, ast.Const):
                node.external = False
                externals.remove(node)
                externals.update(node for node in node.getChildNodes() if node.external and not node.constant)
    def dispatch(translator, node):
        node.external = node.constant = None
        ASTTranslator.dispatch(translator, node)
        children = node.getChildNodes()
        if node.external is None and children and all(
                getattr(child, 'external', False) and not getattr(child, 'raw_sql', False) for child in children):
            node.external = True
        if node.external and not node.constant:
            externals = translator.externals
            externals.difference_update(children)
            externals.add(node)
    def preGenExprInner(translator, node):
        translator.contexts.append(set())
        dispatch = translator.dispatch
        for i, qual in enumerate(node.quals):
            dispatch(qual.iter)
            dispatch(qual.assign)
            for if_ in qual.ifs: dispatch(if_.test)
        dispatch(node.expr)
        translator.contexts.pop()
        return True
    def preLambda(translator, node):
        if node.varargs or node.kwargs or node.defaults: throw(NotImplementedError)
        translator.contexts.append(set(node.argnames))
        translator.dispatch(node.code)
        translator.contexts.pop()
        return True
    def postAssName(translator, node):
        if node.flags != 'OP_ASSIGN': throw(TypeError)
        name = node.name
        if name.startswith('__'): throw(TranslationError, 'Illegal name: %r' % name)
        translator.contexts[-1].add(name)
    def postName(translator, node):
        name = node.name
        for context in translator.contexts:
            if name in context: return
        node.external = True
    def postConst(translator, node):
        node.external = node.constant = True
    def postDict(translator, node):
        node.external = True
    def postList(translator, node):
        node.external = True
    def postKeyword(translator, node):
        node.constant = node.expr.constant
    def postCallFunc(translator, node):
        func_node = node.node
        if not func_node.external: return
        attrs = []
        while isinstance(func_node, ast.Getattr):
            attrs.append(func_node.attrname)
            func_node = func_node.expr
        if not isinstance(func_node, ast.Name): return
        attrs.append(func_node.name)
        expr = '.'.join(reversed(attrs))
        x = eval(expr, translator.globals, translator.locals)
        try: hash(x)
        except TypeError: pass
        else:
            if x in translator.special_functions:
                if x.__name__ == 'raw_sql': node.raw_sql = True
                elif x is getattr:
                    attr_node = node.args[1]
                    attr_node.parent_node = node
                    translator.getattr_nodes.add(attr_node)
                else: node.external = False
            elif x in translator.const_functions:
                for arg in node.args:
                    if not arg.constant: return
                if node.star_args is not None and not node.star_args.constant: return
                if node.dstar_args is not None and not node.dstar_args.constant: return
                node.constant = True

getattr_cache = {}
extractors_cache = {}

def create_extractors(code_key, tree, filter_num, globals, locals,
                      special_functions, const_functions, additional_internal_names=()):
    result = None
    getattr_key = code_key, filter_num
    getattr_extractors = getattr_cache.get(getattr_key)
    if getattr_extractors:
        getattr_attrname_values = tuple(eval(code, globals, locals) for src, code in getattr_extractors)
        extractors_key = (code_key, filter_num, getattr_attrname_values)
        try:
            result = extractors_cache.get(extractors_key)
        except TypeError:
            pass # unhashable type
    if not result:
        pretranslator = PreTranslator(
            tree, globals, locals, special_functions, const_functions, additional_internal_names)

        extractors = {}
        for node in pretranslator.externals:
            src = node.src = ast2src(node)
            if src == '.0': code = None
            else: code = compile(src, src, 'eval')
            extractors[filter_num, src] = code

        getattr_extractors = {}
        getattr_attrname_values = {}
        for node in pretranslator.getattr_nodes:
            if node in pretranslator.externals:
                code = extractors[filter_num, node.src]
                getattr_extractors[src] = code
                attrname_value = eval(code, globals, locals)
                getattr_attrname_values[src] = attrname_value
            elif isinstance(node, ast.Const):
                attrname_value = node.value
            else: throw(TypeError, '`%s` should be either external expression or constant.' % ast2src(node))
            if not isinstance(attrname_value, basestring): throw(TypeError,
                '%s: attribute name must be string. Got: %r' % (ast2src(node.parent_node), attrname_value))
            node._attrname_value = attrname_value
        getattr_cache[getattr_key] = tuple(sorted(getattr_extractors.items()))

        varnames = list(sorted(extractors))
        getattr_attrname_values = tuple(val for key, val in sorted(getattr_attrname_values.items()))
        extractors_key = (code_key, filter_num, getattr_attrname_values)
        result = extractors_cache[extractors_key] = extractors, varnames, tree, extractors_key
    return result
