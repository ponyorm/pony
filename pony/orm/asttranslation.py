from compiler import ast
from functools import update_wrapper

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
    try: PythonTranslator(tree)
    except NotImplementedError: return repr(tree)
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
        lower = node.lower is not None and node.lower.src or ''
        upper = node.upper is not None and node.upper.src or ''
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
    def __init__(translator, tree, additional_internal_names=()):
        ASTTranslator.__init__(translator, tree)
        translator.additional_internal_names = additional_internal_names
        translator.contexts = []
        translator.externals = externals = set()
        translator.dispatch(tree)
        for node in externals.copy():
            if isinstance(node, nonexternalizable_types):
                node.external = False
                externals.remove(node)
                externals.update(node.getChildNodes())
    def dispatch(translator, node):
        node.external = node.constant = False
        ASTTranslator.dispatch(translator, node)
        childs = node.getChildNodes()
        if childs and all(getattr(child, 'external', False) for child in childs):
            node.external = True
        if node.external and not node.constant:
            externals = translator.externals
            externals.difference_update(childs)
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
        if name == 'random': return
        node.external = name not in translator.additional_internal_names
    def postConst(translator, node):
        node.external = node.constant = True

extractors_cache = {}

def create_extractors(code_key, tree, additional_internal_names=()):
    result = extractors_cache.get(code_key)
    if result is None:
        pretranslator = PreTranslator(tree, additional_internal_names)
        extractors = {}
        for node in pretranslator.externals:
            src = node.src = ast2src(node)
            if src == '.0': code = None
            else: code = compile(src, src, 'eval')
            extractors[src] = code
        varnames = list(sorted(extractors))
        result = extractors_cache[code_key] = extractors, varnames, tree
    return result
