import threading
from functools import wraps

class RetryManager:
    def __init__(self):
        self.failed_items = []
        self.lock = threading.Lock()
        pass

    def track_execution(self, max_retries=int):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                for attemp in range(max_retries+1):
                    try:
                        result = func(*args, **kwargs)
                        return result
                    except Exception as error:
                        
                        if attemp < max_retries:
                            continue

                        with self.lock:
                            error_data = {
                                'error': str(error),
                                'process_id': args[0].process_id,
                                'file_path': args[0].file,
                                'additional_files': ','.join(args[0].additional_files)
                            }
                            self.failed_items.append(error_data)
                        return None
            return wrapper
        return decorator

    def get_failed_items(self):
        return self.failed_items

    def clean_failed_items(self) -> None:
        return self.failed_items.clear()

retry_manager = RetryManager()
retry_manager.clean_failed_items()
