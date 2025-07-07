import time
from functools import wraps
from typing import Callable, TypeVar, Optional
from config.logging_config import setup_logger

logger = setup_logger(__name__)

T = TypeVar('T')

def retry_on_failure(
    retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable] = None
) -> Callable:
    """Retry decorator with exponential backoff"""
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            retry_count = 0
            current_delay = delay

            while retry_count < retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retry_count += 1
                    if retry_count == retries:
                        logger.error(f"Max retries ({retries}) reached for {func.__name__}: {str(e)}")
                        raise

                    logger.warning(
                        f"Attempt {retry_count}/{retries} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {current_delay:.2f} seconds..."
                    )

                    if on_retry:
                        on_retry(retry_count, e, current_delay)

                    time.sleep(current_delay)
                    current_delay *= backoff

        return wrapper
    return decorator