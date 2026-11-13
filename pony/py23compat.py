import platform
import sys
from typing import Any, Protocol


class Comparable(Protocol):
    def __lt__(self, other: Any) -> bool: ...
    def __gt__(self, other: Any) -> bool: ...


PYPY: bool = platform.python_implementation() == "PyPy"
PY36: bool = sys.version_info[:2] >= (3, 6)
PY37: bool = sys.version_info[:2] >= (3, 7)
PY38: bool = sys.version_info[:2] >= (3, 8)
PY39: bool = sys.version_info[:2] >= (3, 9)
PY310: bool = sys.version_info[:2] >= (3, 10)
PY311: bool = sys.version_info[:2] >= (3, 11)
PY312: bool = sys.version_info[:2] >= (3, 12)
PY313: bool = sys.version_info[:2] >= (3, 13)
PY314: bool = sys.version_info[:2] >= (3, 14)

unicode: type[str] = str
buffer: type[bytes] = bytes
int_types: tuple[type[int]] = (int,)


def cmp(a: Comparable, b: Comparable) -> int:
    return (a > b) - (a < b)
