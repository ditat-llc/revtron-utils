from typing import Callable
from functools import wraps
import time


def timer(_fn: Callable | None = None, *, text: str | None = None) -> Callable:
    if _fn is None:
        def decorator(fn: Callable) -> Callable:
            @wraps(fn)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                result = fn(*args, **kwargs)
                print(fn.__qualname__, time.time() - start_time, text or '')
                return result
            return wrapper
        return decorator
    else:
        @wraps(_fn)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = _fn(*args, **kwargs)
            print(_fn.__qualname__, time.time() - start_time, text or '')
            return result
        return wrapper


if __name__ == '__main__':
    class F:
        @timer
        def do(self, i, a=1):
            print('hello', a, i)

    f = F()
    f.do(i=8, a=3)


