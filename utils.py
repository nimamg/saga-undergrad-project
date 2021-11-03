import threading


def use_thread(f):
    def wrapper(*args, **kwargs):
        print(f'Starting thread to run {f.__name__}')
        t = threading.Thread(target=f, args=args, kwargs=kwargs)
        t.start()
        return
    return wrapper
