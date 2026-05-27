import os
settings = {
    'provider': 'mysql',
    'host': os.environ.get('PONY_TEST_HOST', 'localhost'),
    'user': 'ponytest',
    'passwd': 'ponytest',
    'db': 'pony_test',
}
