import asyncio
import time
import functools
import logging

def retry(
    max_retries: int = 3,
    backoff_factor: float = 2,
    exceptions: tuple = (Exception,),
):
    """
    Retry decorator supporting both synchronous and asynchronous functions,
    with exponential backoff.

    Parameters
    ----------
    max_retries : int, optional
        Maximum number of attempts before the exception is raised.
        Defaults to 3.
    backoff_factor : float, optional
        Base multiplier for exponential backoff. The waiting time is computed as::

            wait_time = backoff_factor ** attempt

        Defaults to 2.
    exceptions : tuple of Exception types, optional
        Tuple of exception classes that should trigger a retry.
        Defaults to ``(Exception,)``.

    Returns
    -------
    callable
        A decorator that wraps the target function and applies retry logic.

    Notes
    -----
    - Works for both sync and async functions.
    - Logs retry attempts at WARNING level.
    - The function sleeps using ``time.sleep`` for synchronous functions
      and ``asyncio.sleep`` for asynchronous ones.

    Examples
    --------
    >>> @retry(max_retries=5, backoff_factor=1.5)
    ... def fetch_data():
    ...     ...

    >>> @retry(exceptions=(ValueError,))
    ... async def fetch_async():
    ...     ...
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    wait_time = backoff_factor ** attempt
                    logger.warning(
                        f"[ASYNC RETRY] {func.__name__} failed "
                        f"({attempt}/{max_retries}): {e}. Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    wait_time = backoff_factor ** attempt
                    logger.warning(
                        f"[SYNC RETRY] {func.__name__} failed "
                        f"({attempt}/{max_retries}): {e}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator