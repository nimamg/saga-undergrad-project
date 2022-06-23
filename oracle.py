import functools
import time
from multiprocessing import Process
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Type
import yaml

from broker import RabbitWrapper
from database import Database
from utils import convert_camel_case_to_snake_case


def on_consume(queue: str):
    def decorator(func):
        func.consumer_data = {'queue': queue}
        return func
    return decorator

# def consumer_callback_wrapper(func):
#     def wrapper(ch, method, props, body):
#


class GetConsumers(type):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        cls.consumers = []
        for method in dct.values():
            if hasattr(method, 'consumer_data'):
                cls.consumers.append({'queue': method.consumer_data['queue'], 'callback': method})


@dataclass
class Message:
    """
    Message class for the message queue
    """
    timestamp: float
    data: dict = field(default_factory=dict)

    def get_queue(self):
        raise NotImplementedError()

    def get_data(self):
        return self.data


class Service(metaclass=GetConsumers):
    UNDECORATED_FUNCTIONS = [
        'publish',
        'consume',
        'overlaps',
    ]

    def __init__(self, rabbit_interface: RabbitWrapper, db, downtime=None):
        print('initializing service parent class')
        self.downtime = downtime
        self.rabbit_interface = rabbit_interface
        self.db = db
        for consumer in self.consumers:
            print(f'running consumer for queue: {consumer["queue"]} in thread')
            self.rabbit_interface.consume_continuously(consumer['queue'], consumer['callback'])

    # @staticmethod
    def publish(self, message):
        print(f'publish called on queue: {message.get_queue()}, data: {message.get_data()}')
        self.rabbit_interface.publish(message.get_queue(), message.get_data())


    # @staticmethod
    def consume(self, queue: str):
        print(f'consume called on queue: {queue}')
        self.rabbit_interface.consume_one(queue, timeout=5)

    @staticmethod
    def overlaps(func_start, func_end, down_start, down_end):
        if down_start <= func_start <= down_end:
            return True
        if down_start <= func_end <= down_end:
            return True
        if func_start <= down_start <= func_end:
            return True
        return False

    def _decorator(self, f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # TODO : Should be fixed
            now = time.time() - self.start_tim
            if (
                self.downtime
                and any([self.overlaps(now, now + 1, dt['start'], dt['end']) for dt in self.downtime])
            ):
                print('down!')
                return
            print('running!')
            return f(*args, **kwargs)
        return wrapper

    def __getattribute__(self, item):
        value = object.__getattribute__(self, item)
        if callable(value) and item not in Service.UNDECORATED_FUNCTIONS:
            decorator = super(Service, self).__getattribute__('_decorator')
            return decorator(value)
        return value


def create_message_class(message_class_name):
    print(f'creating message class : {message_class_name}')

    class CustomMessage(Message):
        def get_queue(self):
            return convert_camel_case_to_snake_case(message_class_name)
    return CustomMessage


def create_method(method_spec):
    print(f'creating method with spec: {method_spec}')
    if method_spec['role'] == 'initiator':
        print('creating initiator method')

        def method(self, id, msg_time):
            print('running initiator method')
            self.db.create_db_record(request_id=id)
            print('created db record')
            for message in self.method_data[method_spec['name']]['publishes']:
                self.publish(
                    create_message_class(message['name'])(
                        timestamp=msg_time,
                        data={'id': id, **message.get('data', {})}
                    )
                )
            try:
                data = self.consume(convert_camel_case_to_snake_case(self.method_data[method_spec['name']]['consumes']))  # todo: fix this
                return data['status'] == 'success'
            except:
                for rollback in self.method_data[method_spec['name']]['rollbacks']:
                    self.publish(create_message_class(rollback['name'])(timestamp=time.time(), data={'id': id}))
                return False
    elif method_spec['role'] == 'participant':
        print('creating participant method')

        @on_consume(convert_camel_case_to_snake_case(method_spec['trigger']))
        def method(self, id, msg_time):
            print('running participant method')
            self.db.create_db_record(request_id=id)
            print('created db record')
            for message in self.method_data[method_spec['name']]['publishes']:
                self.publish(
                    create_message_class(message['name'])(
                        timestamp=msg_time,
                        data={'id': id, **message.get('data', {})}
                    )
                )
    elif method_spec['role'] == 'rollback':
        print('creating rollback method')

        @on_consume(convert_camel_case_to_snake_case(method_spec['trigger']))
        def method(self, id, msg_time):
            self.db.update_db_record(id, status='rollback')
            for message in self.method_data[method_spec['name']]['publishes']:
                self.publish(
                    create_message_class(message['name'])(
                        timestamp=msg_time,
                        data={'id': id, **message.get('data', {})}
                    )
                )
    else:
        raise Exception('Wrong method type')
    return method


class DbRecord:
    def __init__(self, request_id, _id):
        self.request_id = request_id
        self.id = _id
        self.status = 'committed'

    def rollback(self, db):
        db.update_db_record(self.id, status='rolled_back')


def run_service(data):
    print(f'running service with data: {data}')

    class GenericDB(Database):
        class Meta:
            objs = (
                DbRecord,
            )

    class CustomService(Service):
        def __init__(self, rabbit_interface: RabbitWrapper, db, downtime=None):
            super().__init__(rabbit_interface, db, downtime)
            print(f'init called on custom service: {data}')
            self.name = data['name']
            self.method_data = defaultdict(dict)

            for method_specification in data['methods']:
                setattr(self, method_specification['name'], create_method(method_specification))
                if method_specification['role'] == 'initiator':
                    self.initiate = getattr(self.__class__, method_specification['name'])
                self.method_data[method_specification['name']]['publishes'] = method_specification['publishes']
                self.method_data[method_specification['name']]['rollbacks'] = method_specification['rollbacks']

    print('service created')
    print([_ for _ in dir(CustomService) if not _.startswith('_')])

    service = CustomService(RabbitWrapper(), GenericDB(f'{data["name"]}_db'), data['downtime'])
    return service
    # if any([method_spec['role'] == 'initiator' for method_spec in data['methods']]):
    #     service.initiate()


def run_oracle():
    service_processes = {}
    with open('spec.yml', 'r') as f:
        data = yaml.safe_load(f)
    for service_data in data['services']:
        service_processes[service_data['name']] = Process(target=run_service, args=(service_data,))
        service_processes[service_data['name']].start()
    print('all processes ran')
    time.sleep(10)
    print('terminating processes')
    for service_name, service_process in service_processes.items():
        service_process.terminate()


class A(Service):
    def a(self):
        print('xxx')
