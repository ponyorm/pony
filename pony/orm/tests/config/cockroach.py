import os
settings = {
    'provider': 'cockroach',
    'host': os.environ.get('PONY_TEST_HOST', 'localhost'),
    'port': 26257,
    'user': 'root',
    'database': 'defaultdb',
    'sslmode': 'disable',
}
