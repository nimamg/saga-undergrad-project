import random
from os.path import exists
from typing import Tuple
import pickle

from utils import convert_camel_case_to_snake_case


class ObjectDoesNotExist(Exception):
    pass


class Database:
    class Meta:
        objs: Tuple[type] = tuple()

    def __init__(self, db_name, random_id=False):
        self.db_name = db_name
        self.storages = []
        self.ids = set()
        self.random_id = random_id
        for obj in self.Meta.objs:
            obj_name = self._get_obj_name(obj)
            storage_name = f'{obj_name}_storage'
            self.storages.append(storage_name)
            seq_name = f'{obj_name}_pk_seq'
            setattr(self.__class__, storage_name, {})
            setattr(self.__class__, seq_name, 1)
            setattr(self.__class__, f'get_{obj_name}', self._getter_function(storage_name))
            setattr(self.__class__, f'create_{obj_name}', self._creator_function(obj, seq_name, storage_name))
            setattr(self.__class__, f'update_{obj_name}', self._updater_function(storage_name))
            setattr(self.__class__, f'delete_{obj_name}', self._deleter_function(storage_name))
        # self.read_from_storage()

    def persist(self):
        data = {}
        for obj in self.Meta.objs:
            obj_name = self._get_obj_name(obj)
            storage_name = f'{obj_name}_storage'
            seq_name = f'{obj_name}_pk_seq'
            data[storage_name] = getattr(self, storage_name)
            data[seq_name] = getattr(self, seq_name)
        # print(f'persist, data: {data}')
        with open(f'{self.db_name}.pickle', 'wb') as f:
            pickle.dump(data, f)

    def read_from_storage(self):
        # print('read_from_storage')
        if exists(f'{self.db_name}.pickle'):
            # print('file exists')
            with open(f'{self.db_name}.pickle', 'rb') as f:
                data = pickle.load(f)
                for k, v in data.items():
                    # print(f'setting {k} to {v}')
                    setattr(self, k, v)
            return
        # print('file not exists')

    def print_objs(self):
        for storage_name in self.storages:
            print(f'**** {storage_name} ****')
            storage = getattr(self, storage_name)
            for obj in storage.values():
                print(vars(obj))

    @staticmethod
    def _get_obj_name(obj):
        return convert_camel_case_to_snake_case(obj.__name__)

    def _creator_function(self, obj, seq_name, storage_name):
        def creator(self, *args, **kwargs):
            # _id = getattr(self, seq_name)
            _id = self.generate_id(seq_name)
            storage = getattr(self, storage_name)
            res = obj(*args, **kwargs, _id=_id)
            storage[_id] = res
            # setattr(self, seq_name, _id + 1)
            # self.persist()
            return res
        return creator

    def generate_id(self, seq_name=None):
        if self.random_id:
            while (_id := random.randint(1, 1000000)) in self.ids:
                continue
            self.ids.add(_id)
        else:
            _id = getattr(self, seq_name)
            setattr(self, seq_name, _id + 1)
        return _id

    def _getter_function(self, storage_name):
        def getter(self, _id):
            # self.read_from_storage()
            storage = getattr(self, storage_name)
            try:
                return storage[_id]
            except KeyError:
                raise ObjectDoesNotExist()
        return getter

    def _updater_function(self, storage_name):
        def updater(self, _id, **kwargs):
            storage = getattr(self, storage_name)
            # print(f'updater, storage name:{storage_name}, storage: {storage}, _id: {_id}, kwargs: {kwargs}')
            # self.read_from_storage()
            storage = getattr(self, storage_name)
            # print(f'updater, storage name:{storage_name}, storage: {storage}, _id: {_id}, kwargs: {kwargs}')
            for field, val in kwargs.items():
                try:
                    setattr(storage[_id], field, val)
                except KeyError:
                    raise ObjectDoesNotExist()
            # self.persist()
        return updater

    def _deleter_function(self, storage_name):
        def deleter(self, _id):
            # self.read_from_storage()
            storage = getattr(self, storage_name)
            try:
                del storage[_id]
            except KeyError:
                raise ObjectDoesNotExist()
            # self.persist()
        return deleter

