import json
from time import sleep

from broker import RabbitWrapper, INVENTORY_ORDER_QUEUE, INVENTORY_ROLLBACK_QUEUE, FINANCIAL_ROLLBACK_QUEUE, ORDER_SERVICE_RESPONSE_QUEUE
from database import Database
from utils import use_thread


class InsufficientResourceException(Exception):
    def __init__(self):
        super(InsufficientResourceException, self).__init__('Insufficient resources')


class Stock:
    def __init__(self, quantity=100, _id=None):
        self.quantity = quantity
        self.id = _id


class SellRecord:
    def __init__(self, quantity, order_id, _id):
        self.quantity = quantity
        self.id = _id
        self.order_id = order_id


class SellRecordNotFoundException(Exception):
    pass


class InventoryDatabase(Database):
    class Meta:
        objs = (
            Stock,
            SellRecord,
        )

    def get_sell_record_by_order_id(self, order_id):
        self.read_from_storage()
        for sell_record in self.sell_record_storage.values():
            print('getting sell record by order id')
            if sell_record.order_id == order_id:
                return sell_record
        else:
            raise SellRecordNotFoundException()


class InventoryService:
    def __init__(self, db: InventoryDatabase, rabbit_interface: RabbitWrapper):
        self.rabbit_interface = rabbit_interface
        self.db = db

        self.stock = self.db.create_stock()

    # @use_thread
    def start_place_order_consumer(self):
        self.rabbit_interface.consume_continuously(INVENTORY_ORDER_QUEUE, self.place_order)

    # @use_thread
    def start_rollback_order_consumer(self):
        self.rabbit_interface.consume_continuously(INVENTORY_ROLLBACK_QUEUE, self.process_rollback_message)

    # @use_thread
    def place_order(self, ch, method, props, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        message = json.loads(body)

        print('Inventory service: placing order')
        stock = self.db.get_stock(self.stock.id)
        if stock.quantity < message['quantity']:
            print('Inventory service, failure, rolling back order')
            self.rabbit_interface.publish(INVENTORY_ROLLBACK_QUEUE, json.dumps({'order_id': message['order_id']}))
            # self.financial_service.rollback_order(order_id)
            return

        self.db.create_sell_record(message['order_id'], message['quantity'])
        # self.sold_quantity[message['order_id']] = message['quantity']
        # self.total_quantity -= message['quantity']
        self.db.update_stock(self.stock.id, quantity=stock.quantity - message['quantity'])
        try:
            self.rabbit_interface.publish(
                ORDER_SERVICE_RESPONSE_QUEUE.format(
                    order_id=message['order_id']
                ),
                json.dumps({
                    'order_id': message['order_id'],
                    'status': 'success'
                }),
                queue_auto_delete=True,
            )
            # self.order_service.complete_order(order_id)
        except:
            self.rollback_order(message['order_id'])

    # @use_thread
    def process_rollback_message(self, ch, method, props, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        message = json.loads(body)
        self.rollback_order(message['order_id'])

    def rollback_order(self, order_id):
        sell_record = self.db.get_sell_record_by_order_id(order_id)
        current_quantity = self.db.get_stock(self.stock.id).quantity
        self.db.update_stock(self.stock.id, quantity=current_quantity + sell_record.quantity)
        self.db.delete_sell_record(sell_record.id)
        # publish to financial service
        self.rabbit_interface.publish(FINANCIAL_ROLLBACK_QUEUE, json.dumps({'order_id': order_id}))
