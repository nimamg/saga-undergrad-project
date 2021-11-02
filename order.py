from time import sleep
import threading

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
        self.status = 'pending'


class OrderDatabase(Database):
    class Meta:
        objs = (
            Order,
        )


class OrderService:
    def __init__(self, db: OrderDatabase, financial_service=None):
        self.financial_service = financial_service
        self.db = db

    def place_order(self, quantity, account_id):
        print('Order Service: placing order')
        order = self.db.create_order(quantity)
        try:
            print('Order Service: calling financial')
            self.financial_service.place_order(order.id, order.price, account_id, quantity)
        except Exception as e:
            print('Order Service: calling financial failed, ', str(e))
            return 'Failure'
        order_id = order.id
        timer = TIMEOUT_SECONDS
        while timer > 0:
            order = self.db.get_order(order_id)
            if order.status == SUCCESSFUL:
                return 'Success'
            elif order.status == REJECTED:
                return 'Failure'
            sleep(POLL_INTERVAL_SECONDS)
            timer -= POLL_INTERVAL_SECONDS
        else:
            self.financial_service.rollback_order(order.id)
            self.reject_order(order.id)
            raise Exception('Service Not Available')

    def _complete_order(self, order_id):
        print('Order Service: Completing order')
        order = self.db.get_order(order_id)
        if order.status != PENDING:
            raise Exception('Can\'t complete order')
        self.db.update_order(_id=order_id, status=SUCCESSFUL)

    def complete_order(self, order_id):
        print('Order service: starting thread to complete order')
        t = threading.Thread(target=self._complete_order, args=(order_id,))
        t.start()
        return

    def _reject_order(self, order_id):
        print('Order Service: Rejecting order')
        order = self.db.get_order(order_id)
        if order.status == SUCCESSFUL:
            raise Exception
        self.db.update_order(_id=order_id, status=REJECTED)

    def reject_order(self, order_id):
        print('Order service: starting thread to reject order')
        t = threading.Thread(target=self._reject_order, args=(order_id,))
        t.start()
        return
