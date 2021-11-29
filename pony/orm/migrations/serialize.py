from decimal import Decimal
from datetime import *
from uuid import UUID
from pony.py23compat import *
from pony.utils import throw
from pony.orm import core
from pony.orm.ormtypes import IntArray, StrArray, FloatArray, Json, LongStr

import types


def serialize(obj, imports):
    result = None
    t = obj if isinstance(obj, type) else type(obj)
    if t in (str, int, float, bool):
        pass
    elif t == Decimal:
        imports['decimal'].add('Decimal')
    elif t in (datetime, date, time, timedelta):
        imports['datetime'].add(t.__name__)
        result = repr(obj)
        if result.startswith('datetime.'):  # initial value
            result = result[len('datetime.'):]
        elif result.startswith('<class'):
            result = t.__name__
    elif t == UUID:
        imports['uuid'].add('UUID')
    elif t == buffer:
        if PY2:
            imports['pony.py23compat'].add('buffer')
    elif t == Json:
        imports['pony.orm'].add('Json')
    elif t == LongStr:
        imports['pony.orm'].add('LongStr')
    elif t in (IntArray, StrArray, FloatArray):
        imports['pony.orm'].add(t.__name__)
    elif t in (types.BuiltinMethodType, types.MethodType):
        # like datetime.now as value
        if obj is t: throw(core.MigrationError, 'Cannot serialize method: %r' % obj)
        method = obj
        module_name = method.__self__.__module__
        class_name = method.__self__.__name__
        func_name = method.__name__
        imports[module_name].add(class_name)
        result = '%s.%s' % (class_name, func_name)
    elif t in (types.FunctionType, types.BuiltinFunctionType):
        if obj is t: throw(core.MigrationError, 'Cannot serialize function: %r' % obj)
        func = obj
        module_name = func.__module__
        func_name = func.__name__
        imports[module_name].add(func_name)
        result = '%s.%s' % (module_name, func_name)
        # TODO
        # from module_name import func_name
        # module_name.func_name(...)
        # looks incorrect
    else:
        print(t, obj)
        throw(NotImplementedError)

    if result is None:
        result = t.__name__ if obj is t else repr(obj)
    return result
