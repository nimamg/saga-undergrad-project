import json

from broker import RabbitWrapper, FINANCIAL_ORDER_QUEUE, TimeoutException, ORDER_SERVICE_RESPONSE_QUEUE, FINANCIAL_ROLLBACK_QUEUE, INVENTORY_ROLLBACK_QUEUE
from database import Database

PENDING = 'pending'
SUCCESSFUL = 'successful'
REJECTED = 'rejected'

PRICE = 100
TIMEOUT_SECONDS = 5
POLL_INTERVAL_SECONDS = 0.1


class Order:
    def __init__(self, quantity, _id=None):
        self.id = _id
        self.quantity = quantity
        self.price = quantity * PRICE
        self.status = PENDING


class OrderDatabase(Database):
    class Meta:
        objs = (
            Order,
        )


class OrderService:
    def __init__(self, db: OrderDatabase, rabbit_interface: RabbitWrapper):
        self.db = db
        self.rabbit_interface = rabbit_interface

    def place_order(self, quantity, account_id):
        print('Order Service: placing order')
        order = self.db.create_order(quantity)
        try:
            print('Order Service: calling financial')
            message = {
                'order_id': order.id,
                'account_id': account_id,
                'order_price': order.price,
                'quantity': order.quantity,
            }
            self.rabbit_interface.publish(FINANCIAL_ORDER_QUEUE, json.dumps(message))
        except Exception as e:
            print('Order Service: publishing financial order failed, ', str(e))
            return 'Failure'
        try:
            response = json.loads(self.rabbit_interface.consume_one(
                ORDER_SERVICE_RESPONSE_QUEUE.format(order_id=order.id), timeout=TIMEOUT_SECONDS))
            # print(response)
            if response['status'] == 'success':
                self.complete_order(order.id)
                return 'Success'
            else:
                self.reject_order(order.id)
                return 'Failure'
        except TimeoutException:
            self.reject_order(order.id)
            return 'Failure'

    def complete_order(self, order_id):
        print('Order Service: Completing order')
        order = self.db.get_order(order_id)
        if order.status != PENDING:
            raise Exception('Can\'t complete order')
        self.db.update_order(_id=order_id, status=SUCCESSFUL)

    def reject_order(self, order_id):
        print('Order Service: Rejecting order')
        self.rabbit_interface.publish(FINANCIAL_ROLLBACK_QUEUE, json.dumps({'order_id': order_id}))
        self.rabbit_interface.publish(INVENTORY_ROLLBACK_QUEUE, json.dumps({'order_id': order_id}))
        order = self.db.get_order(order_id)
        if order.status == SUCCESSFUL:
            raise Exception
        self.db.update_order(_id=order_id, status=REJECTED)
