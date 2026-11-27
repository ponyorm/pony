#!/usr/bin/env python

"""Run the Pony ORM and utility test suites on Python 3.10 through 3.14.

Pyright and mypy are run for every one of these versions as well.
Pass versions to restrict the run, e.g. `run_tests.py 3.10 3.14`, and
--no-pyright or --no-mypy to skip a type check.
"""


import argparse
import os
import shutil
import subprocess
import sys

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_VERSIONS = ('3.10', '3.11', '3.12', '3.13', '3.14')
SKIPPED = 'skipped'
TEST_SUITES = (
    (
        'ORM',
        ('-m', 'unittest', 'discover', '-s', 'pony/orm/tests',
         '-p', 'test_*.py'),
    ),
    (
        'Utils',
        ('-m', 'unittest', 'discover', '-s', 'pony/utils/tests',
         '-p', 'test_*.py'),
    ),
)
TYPE_CHECKERS = (
    # name, module, option that selects the Python version
    ('Pyright', 'pyright', '--pythonversion'),
    ('Mypy', 'mypy', '--python-version'),
)


def run_command(command, capture_output=False):
    kwargs = {'cwd': ROOT_DIR}
    if capture_output:
        kwargs.update(
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
    return subprocess.run(command, **kwargs)


def get_python_version(executable):
    command = [
        executable,
        '-c',
        'import sys; print("%d.%d" % sys.version_info[:2])',
    ]
    try:
        result = run_command(command, capture_output=True)
    except OSError:
        return None
    if result.returncode != 0:
        return None
    lines = result.stdout.strip().splitlines()
    return lines[-1] if lines else None


def normalize_executable(executable):
    executable = executable.strip()
    if os.path.isfile(executable):
        return os.path.abspath(executable)
    return shutil.which(executable) or executable


def find_python(version):
    candidates = []

    current_version = '%d.%d' % sys.version_info[:2]
    if current_version == version:
        candidates.append(sys.executable)

    uv = shutil.which('uv')
    if uv:
        result = run_command(
            [uv, 'python', 'find', version],
            capture_output=True,
        )
        if result.returncode == 0:
            lines = result.stdout.strip().splitlines()
            if lines:
                candidates.append(lines[-1])

    if os.name == 'nt':
        launcher = shutil.which('py')
        if launcher:
            result = run_command(
                [
                    launcher,
                    '-' + version,
                    '-c',
                    'import sys; print(sys.executable)',
                ],
                capture_output=True,
            )
            if result.returncode == 0:
                lines = result.stdout.strip().splitlines()
                if lines:
                    candidates.append(lines[-1])

    candidates.extend(('python' + version, 'python' + version + '.exe'))

    checked = set()
    for candidate in candidates:
        executable = normalize_executable(candidate)
        normalized = os.path.normcase(executable)
        if normalized in checked:
            continue
        checked.add(normalized)
        if get_python_version(executable) == version:
            return executable
    return None


def run_suite(executable, arguments):
    command = [executable] + list(arguments)
    print('> %s' % subprocess.list2cmdline(command), flush=True)
    try:
        return run_command(command).returncode
    except OSError as exc:
        print('Cannot run interpreter: %s' % exc, file=sys.stderr)
        return 1


def type_checker_available(module):
    command = [sys.executable, '-m', module, '--version']
    try:
        return run_command(command, capture_output=True).returncode == 0
    except OSError:
        return False


def run_type_checker(module, version_option, version):
    # the type checkers check against the typeshed stubs of the requested
    # version, so they do not need an interpreter of that version
    return run_suite(sys.executable, ('-m', module, version_option, version))


def format_status(exit_code):
    if exit_code is None:
        return 'NOT FOUND'
    if exit_code == SKIPPED:
        return 'SKIPPED'
    if exit_code == 0:
        return 'PASS'
    return 'FAIL (%d)' % exit_code


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument(
        'versions',
        nargs='*',
        metavar='VERSION',
        help='Python versions to run (default: %s)' % ' '.join(PYTHON_VERSIONS),
    )
    for name, module, _ in TYPE_CHECKERS:
        parser.add_argument(
            '--no-' + module, action='store_true', help='do not run ' + name
        )
    args = parser.parse_args()
    for version in args.versions:
        if version not in PYTHON_VERSIONS:
            parser.error(
                'unsupported version %r, choose from: %s'
                % (version, ', '.join(PYTHON_VERSIONS))
            )
    return args


def main():
    args = parse_args()
    versions = args.versions or PYTHON_VERSIONS
    selected = [
        checker for checker in TYPE_CHECKERS
        if not getattr(args, 'no_' + checker[1])
    ]
    missing = set()
    for _, module, _ in selected:
        if not type_checker_available(module):
            print(
                '%s is not installed, run: pip install -r requirements-dev.txt'
                % module,
                file=sys.stderr,
            )
            missing.add(module)

    results = []

    for version in versions:
        print('\n=== Python %s ===' % version, flush=True)
        executable = find_python(version)
        suite_results = []
        if executable is None:
            print(
                'Python %s was not found. Install it or make uv/py available.'
                % version,
                file=sys.stderr,
            )
            suite_results = [None] * len(TEST_SUITES)
        else:
            print('Interpreter: %s' % executable, flush=True)
            for suite_name, arguments in TEST_SUITES:
                print('\n--- %s tests ---' % suite_name, flush=True)
                suite_results.append(run_suite(executable, arguments))

        checker_results = []
        for checker in TYPE_CHECKERS:
            name, module, version_option = checker
            if checker not in selected:
                checker_results.append(SKIPPED)
            elif module in missing:
                checker_results.append(None)
            else:
                print('\n--- %s ---' % name, flush=True)
                checker_results.append(
                    run_type_checker(module, version_option, version)
                )
        results.append((version, suite_results + checker_results))

    print('\n=== Summary ===')
    columns = [name for name, _ in TEST_SUITES]
    columns += [name for name, _, _ in TYPE_CHECKERS]
    print('{:<8} {}'.format(
        'Python', ' '.join(f'{column:<12}' for column in columns)
    ))
    for version, exit_codes in results:
        statuses = (format_status(exit_code) for exit_code in exit_codes)
        print(
            f'{version:<8} '
            + ' '.join(f'{status:<12}' for status in statuses)
        )

    return int(any(
        exit_code is None or exit_code not in (0, SKIPPED)
        for _, exit_codes in results
        for exit_code in exit_codes
    ))


if __name__ == '__main__':
    sys.exit(main())
