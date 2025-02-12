import functools
from collections.abc import Callable

from typing_extensions import Concatenate, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")
T_self = TypeVar("T_self")


def copy_function_signature(
    source: Callable[P, T],
) -> Callable[[Callable[..., T]], Callable[P, T]]:
    """Copied from https://github.com/python/typing/issues/270#issuecomment-1541935319"""

    def wrapper(target: Callable[..., T]) -> Callable[P, T]:
        @functools.wraps(source)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            return target(*args, **kwargs)

        return wrapped

    return wrapper


def copy_method_signature(
    source: Callable[Concatenate[T_self, P], T],
) -> Callable[[Callable[..., T]], Callable[P, T]]:
    """Modified from https://github.com/python/typing/issues/270#issuecomment-1541935319"""

    def wrapper(target: Callable[..., T]) -> Callable[P, T]:
        @functools.wraps(source)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            return target(*args, **kwargs)

        return wrapped

    return wrapper
