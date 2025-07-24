from functools import wraps

from aiocache.decorators import cached


def cached_with_force_refresh(
    *cache_args, force_arg_name="force_refresh", **cache_kwargs
):
    """
    Wraps aiocache.cached to add a force-refresh capability via a function argument.

    All functionality of aiocache.cached is preserved.
    """

    def decorator(func):
        cached_call = cached(*cache_args, **cache_kwargs)
        cached_func = cached_call(func)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            force_refresh = kwargs.pop(force_arg_name, False)
            if not force_refresh:
                # Normal cached behavior
                return await cached_func(*args, **kwargs)

            # Force refresh path: compute result, overwrite cache manually
            result = await func(*args, **kwargs)
            key = cached_call.get_cache_key(func, args, kwargs)
            await cached_func.cache.set(key, result, ttl=cache_kwargs.get("ttl"))  # type: ignore
            return result

        return wrapper

    return decorator
