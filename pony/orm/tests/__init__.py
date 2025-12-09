import unittest
import os
import types
import pony.orm.core, pony.options

pony.options.CUT_TRACEBACK = False
pony.orm.core.sql_debug(False)


def _load_env():
    settings_filename = os.environ.get('pony_test_db')
    if settings_filename is None:
        print('use default sqlite provider')
        return dict(provider='sqlite', filename=':memory:')
    with open(settings_filename, 'r') as f:
        content = f.read()

    config = {}
    exec(content, config)
    settings = config.get('settings')
    if settings is None or not isinstance(settings, dict):
        raise ValueError('Incorrect settings pony test db file contents')
    provider = settings.get('provider')
    if provider is None:
        raise ValueError('Incorrect settings pony test db file contents: provider was not specified')
    print('use provider %s' % provider)
    return settings


db_params = _load_env()


def setup_database(db):
    if db.provider is None:
        db.bind(**db_params)
    if db.schema is None:
        db.generate_mapping(check_tables=False)
    db.drop_all_tables(with_all_data=True)
    db.create_tables()


def teardown_database(db):
    if db.schema:
        db.drop_all_tables(with_all_data=True)
    db.disconnect()


def only_for(providers):
    if not isinstance(providers, (list, tuple)):
        providers = [providers]
    def decorator(x):
        if isinstance(x, type) and issubclass(x, unittest.TestCase):
            @classmethod
            def setUpClass(cls):
                raise unittest.SkipTest('%s tests implemented only for %s provider%s' % (
                    cls.__name__, ', '.join(providers), '' if len(providers) < 2 else 's'
                ))
            if db_params['provider'] not in providers:
                x.setUpClass = setUpClass
            result = x
        elif isinstance(x, types.FunctionType):
            def new_test_func(self):
                if db_params['provider'] not in providers:
                    raise unittest.SkipTest('%s test implemented only for %s provider%s' % (
                        x.__name__, ', '.join(providers), '' if len(providers) < 2 else 's'
                    ))
                return x(self)
            result = new_test_func
        else:
            raise TypeError
        return result
    return decorator
