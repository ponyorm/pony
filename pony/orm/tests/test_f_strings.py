from sys import version_info

if version_info[:2] >= (3, 6):
    from pony.orm.tests.py36_test_f_strings import *