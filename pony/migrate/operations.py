
class Op(object):

    def __init__(self, sql, obj, type, prefix=None):
        self.obj = obj
        self.sql = sql
        self.type = type
        self.prefix = prefix

    def __repr__(self):
        try:
            result = self.get_sql()
            assert isinstance(result, str)
            return result
        except:
            return object.__repr__(self)

    def get_sql(self):
        if self.prefix:
            return ' '.join((self.prefix, self.sql))
        return self.sql

    def __add__(self, other):
        assert self.prefix == other.prefix
        sql = ', '.join((self.sql, other.sql))
        types = []
        for obj in (self, other):
            if isinstance(obj.type, (tuple, list)):
                types.extend(obj.type)
            else:
                types.append(obj.type)
        return self.__class__(sql, [self.obj, other.obj], types, self.prefix)

    def __eq__(self, other):
        if not isinstance(other, Op):
            return False
        return self.type == other.type and self.get_sql() == other.get_sql()

    def __hash__(self):
        return hash(
            (self.type, self.get_sql())
        )


def alter_table(table):
    schema = table.schema
    case = schema.case
    quote_name = schema.provider.quote_name
    return ' '.join((
        case('ALTER TABLE'), quote_name(table.name)
    ))



class OperationBatch(list):
    obj = None

    def __init__(self, *args, **kwargs):
        self.type = kwargs.pop('type', None)
        list.__init__(self, *args, **kwargs)


class CustomOp(object):
    type = 'custom'

    def __init__(self, func):
        self.func = func