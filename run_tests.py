#!/usr/bin/env python

"""Run the Pony ORM and utility test suites on Python 3.10 through 3.14.

Pyright is run for every one of these versions as well.
Pass versions to restrict the run, e.g. `run_tests.py 3.10 3.14`, and
--no-pyright to skip the type check.
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


def pyright_available():
    command = [sys.executable, '-m', 'pyright', '--version']
    try:
        return run_command(command, capture_output=True).returncode == 0
    except OSError:
        return False


def run_pyright(version):
    # pyright checks against the typeshed stubs of the requested version, so it
    # does not need an interpreter of that version
    return run_suite(
        sys.executable, ('-m', 'pyright', '--pythonversion', version)
    )


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
    parser.add_argument(
        '--no-pyright', action='store_true', help='do not run pyright'
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
    with_pyright = not args.no_pyright
    if with_pyright and not pyright_available():
        print(
            'pyright is not installed, run: pip install -r requirements-dev.txt',
            file=sys.stderr,
        )
        pyright_missing = True
    else:
        pyright_missing = False

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

        if not with_pyright:
            pyright_result = SKIPPED
        elif pyright_missing:
            pyright_result = None
        else:
            print('\n--- Pyright ---', flush=True)
            pyright_result = run_pyright(version)
        results.append((version,) + tuple(suite_results) + (pyright_result,))

    print('\n=== Summary ===')
    print('{:<8} {:<12} {:<12} {:<12}'.format('Python', 'ORM', 'Utils', 'Pyright'))
    for version, orm_exit, utils_exit, pyright_exit in results:
        print(
            f'{version:<8} {format_status(orm_exit):<12} '
            f'{format_status(utils_exit):<12} {format_status(pyright_exit):<12}'
        )

    return int(any(
        exit_code is None or exit_code not in (0, SKIPPED)
        for _, *exit_codes in results
        for exit_code in exit_codes
    ))


if __name__ == '__main__':
    sys.exit(main())
