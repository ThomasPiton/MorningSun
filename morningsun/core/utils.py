import asyncio
import time
import functools
import logging

def retry(
    max_retries: int = 3,
    backoff_factor: float = 2,
    exceptions: tuple = (Exception,)
):
    """
    Retry decorator supporting sync and async functions.
    Applies exponential backoff.
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
                    logger.warning(f"[ASYNC RETRY] {func.__name__} failed ({attempt}/{max_retries}): {e}. Retrying in {wait_time}s...")
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
                    logger.warning(f"[SYNC RETRY] {func.__name__} failed ({attempt}/{max_retries}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)

        # Detect whether function is async or not
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator