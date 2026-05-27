import os
settings = {
    'provider': 'postgres',
    'host': os.environ.get('PONY_TEST_HOST', 'localhost'),
    'user': 'ponytest',
    'password': 'ponytest',
    'database': 'pony_test',
}
