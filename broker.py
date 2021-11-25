import pika

FINANCIAL_ORDER_QUEUE = 'financial_order_queue'
INVENTORY_ORDER_QUEUE = 'inventory_order_queue'
ORDER_SERVICE_RESPONSE_QUEUE = 'order_service_response_queue_{order_id}'
FINANCIAL_SERVICE_RESPONSE_QUEUE = 'financial_service_response_queue_{order_id}'
FINANCIAL_ROLLBACK_QUEUE = 'financial_rollback_queue'
INVENTORY_ROLLBACK_QUEUE = 'inventory_rollback_queue'


class TimeoutException(Exception):
    pass


class RabbitWrapper:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

    def _declare_queue(self, queue_name, auto_delete=False):
        self.connection.channel().queue_declare(queue=queue_name, auto_delete=auto_delete)

    def publish(self, queue_name, message, queue_auto_delete=False):
        self._declare_queue(queue_name, auto_delete=queue_auto_delete)
        self.connection.channel().basic_publish(exchange='', routing_key=queue_name, body=message)

    def consume_continuously(self, queue_name, callback):
        print('consuming from queue: {}'.format(queue_name))
        self._declare_queue(queue_name)
        # print('declared queue')
        ch = self.connection.channel()
        ch.basic_consume(queue=queue_name, on_message_callback=callback)
        # print('configured callback')
        ch.start_consuming()

    def consume_one(self, queue_name, timeout=None):
        self._declare_queue(queue_name, auto_delete=True)
        ch = self.connection.channel()
        for method, properties, body in ch.consume(queue=queue_name, inactivity_timeout=timeout):
            if body is not None:
                ch.basic_ack(method.delivery_tag)
                return body
            else:
                raise TimeoutException()


