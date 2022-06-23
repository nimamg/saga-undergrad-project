import threading

import pika

from utils import use_thread

FINANCIAL_ORDER_QUEUE = 'financial_order_queue'
INVENTORY_ORDER_QUEUE = 'inventory_order_queue'
ORDER_SERVICE_RESPONSE_QUEUE = 'order_service_response_queue_{order_id}'
FINANCIAL_SERVICE_RESPONSE_QUEUE = 'financial_service_response_queue_{order_id}'
FINANCIAL_ROLLBACK_QUEUE = 'financial_rollback_queue'
INVENTORY_ROLLBACK_QUEUE = 'inventory_rollback_queue'


def delete_queues():
    conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    for queue in (FINANCIAL_ORDER_QUEUE, INVENTORY_ORDER_QUEUE, FINANCIAL_ROLLBACK_QUEUE, INVENTORY_ROLLBACK_QUEUE):
        conn.channel().queue_delete(queue)


class TimeoutException(Exception):
    pass


class RabbitWrapper:
    def __init__(self):
        self.connections = {}

    def get_connection(self, identifier):
        if identifier not in self.connections:
            self.connections[identifier] = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        return self.connections[identifier]

    @staticmethod
    def _declare_queue(connection, queue_name, auto_delete=False):
        connection.channel().queue_declare(queue=queue_name, auto_delete=auto_delete)

    def publish(self, queue_name, message, queue_auto_delete=False):
        connection = self.get_connection(threading.get_ident())
        self._declare_queue(connection, queue_name, auto_delete=queue_auto_delete)
        connection.channel().basic_publish(exchange='', routing_key=queue_name, body=message)

    @use_thread
    def consume_continuously(self, queue_name, callback):
        # print('consuming from queue: {}'.format(queue_name))
        connection = self.get_connection(threading.get_ident())
        self._declare_queue(connection, queue_name)
        # print('declared queue')
        ch = connection.channel()
        ch.basic_consume(queue=queue_name, on_message_callback=callback)
        # print('configured callback')
        ch.start_consuming()

    def consume_one(self, queue_name, timeout=None):
        connection = self.get_connection(threading.get_ident())
        self._declare_queue(connection, queue_name, auto_delete=True)
        ch = connection.channel()
        for method, properties, body in ch.consume(queue=queue_name, inactivity_timeout=timeout):
            if body is not None:
                ch.basic_ack(method.delivery_tag)
                return body
            else:
                raise TimeoutException()


