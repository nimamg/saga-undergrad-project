import re
from typing import Tuple


class Database:
    class Meta:
        objs: Tuple[type] = tuple()

    def __init__(self):
        for obj in self.Meta.objs:
            obj_name = self._get_obj_name(obj)
            storage_name = f'{obj_name}_storage'
            seq_name = f'{obj_name}_pk_seq'
            setattr(self.__class__, storage_name, {})
            setattr(self.__class__, seq_name, 1)
            setattr(self.__class__, f'get_{obj_name}', self._getter_function(storage_name))
            setattr(self.__class__, f'create_{obj_name}', self._creator_function(obj, seq_name, storage_name))
            setattr(self.__class__, f'update_{obj_name}', self._updater_function(storage_name))

    @staticmethod
    def _get_obj_name(obj):
        return re.sub('(?!^)([A-Z]+)', r'_\1', obj.__name__).lower()

    def _creator_function(self, obj, seq_name, storage_name):
        def creator(self, *args, **kwargs):
            _id = getattr(self, seq_name)
            storage = getattr(self, storage_name)
            res = obj(*args, **kwargs, _id=_id)
            storage[_id] = res
            setattr(self, seq_name, _id + 1)
            return res
        return creator

    def _getter_function(self, storage_name):
        def getter(self, _id):
            storage = getattr(self, storage_name)
            return storage[_id]
        return getter

    def _updater_function(self, storage_name):
        def updater(self, _id, **kwargs):
            storage = getattr(self, storage_name)
            for field, val in kwargs.items():
                setattr(storage[_id], field, val)
        return updater
