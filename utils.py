import re
import threading


def use_thread(f):
    def wrapper(*args, **kwargs):
        # print(f'Starting thread to run {f.__name__}')
        t = threading.Thread(target=f, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return
    return wrapper


def convert_camel_case_to_snake_case(string):
    return re.sub('(?!^)([A-Z]+)', r'_\1', string).lower()


class ServiceWithDowntime:
    def __init__(self, downtimes):
        self.downtimes = downtimes

    @staticmethod
    def overlaps(func_start, func_end, down_start, down_end):
        if down_start <= func_start <= down_end:
            return True
        if down_start <= func_end <= down_end:
            return True
        if func_start <= down_start <= func_end:
            return True
        return False

    @staticmethod
    def contains(desired_point, interval_start, interval_end):
        if interval_start <= desired_point <= interval_end:
            return True
        return False

    def is_available_during(self, func_start, func_end):
        for downtime in self.downtimes:
            if self.overlaps(func_start, func_end, downtime['start'], downtime['end']):
                return False
        return True

    def is_available_to_start(self, func_start):
        for downtime in self.downtimes:
            if self.contains(func_start, downtime['start'], downtime['end']):
                return False
        return True

    def is_available_until_end(self, func_start, func_end):
        for downtime in self.downtimes:
            if self.contains(downtime['start'], func_start, func_end):
                return False
        return True
