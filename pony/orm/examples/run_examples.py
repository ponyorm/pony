"""Run all ORM examples to verify they work across Python versions."""
import subprocess
import sys
import os
import tempfile

EXAMPLES_DIR = os.path.dirname(os.path.abspath(__file__))

EXAMPLES = [
    'demo.py',
    'estore.py',
    'numbers.py',
    'compositekeys.py',
    'inheritance1.py',
    'university1.py',
    'university2.py',
]

if __name__ == '__main__':
    failed = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for example in EXAMPLES:
            path = os.path.join(EXAMPLES_DIR, example)
            print(f'Running {example}...', flush=True)
            # Use -c so the examples dir isn't added to sys.path, which would
            # cause pony's `import numbers` to find numbers.py (the example)
            # instead of the stdlib numbers module.
            script = f"exec(compile(open({repr(path)}).read(), {repr(path)}, 'exec'), {{'__name__': '__main__', '__file__': {repr(path)}}})"
            result = subprocess.run([sys.executable, '-c', script], cwd=tmpdir)
            if result.returncode != 0:
                print(f'FAILED: {example}', file=sys.stderr)
                failed.append(example)

    if failed:
        print(f'\n{len(failed)} example(s) failed: {", ".join(failed)}', file=sys.stderr)
        sys.exit(1)
    print(f'\nAll {len(EXAMPLES)} examples passed.')
