
class Op(object):

    def __init__(self, sql, obj, type, prefix=None):
        self.obj = obj
        self.sql = sql
        self.type = type
        self.prefix = prefix

    def get_sql(self):
        return self.sql if not self.prefix else '%s %s' % (self.prefix, self.sql)


def alter_table(table):
    schema = table.schema
    quote_name = schema.provider.quote_name
    return '%s %s' % ('ALTER TABLE', quote_name(table.name))


class OperationBatch(list):
    obj = None

    def __init__(self, *args, **kwargs):
        self.type = kwargs.pop('type', None)
        list.__init__(self, *args, **kwargs)


class CustomOp(object):
    type = 'custom'

    def __init__(self, func):
        self.func = func