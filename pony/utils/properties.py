from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

if TYPE_CHECKING:
    from collections.abc import Callable

    from typing_extensions import Self

_T = TypeVar("_T")


class cached_property(Generic[_T]):
    """
    A property that is only computed once per instance and then replaces itself
    with an ordinary attribute. Deleting the attribute resets the property.
    Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
    """

    func: Callable[..., _T]

    def __init__(self, func: Callable[..., _T]) -> None:
        self.__doc__ = func.__doc__
        self.func = func

    @overload
    def __get__(self, obj: None, cls: Any) -> Self: ...
    @overload
    def __get__(self, obj: object, cls: Any) -> _T: ...
    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


class class_property(Generic[_T]):
    """
    Read-only class property
    """

    func: Callable[..., _T]

    def __init__(self, func: Callable[..., _T]) -> None:
        self.func = func

    def __get__(self, instance: Any, cls: type) -> _T:
        return self.func(cls)


class class_cached_property(Generic[_T]):
    func: Callable[..., _T]

    def __init__(self, func: Callable[..., _T]) -> None:
        self.func = func

    def __get__(self, obj: Any, cls: type) -> _T:
        value = self.func(cls)
        setattr(cls, self.func.__name__, value)
        return value
