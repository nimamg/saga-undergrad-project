import threading
from time import sleep


class InsufficientResourceException(Exception):
    def __init__(self):
        super(InsufficientResourceException, self).__init__('Insufficient resources')


class InventoryService:
    def __init__(self, total_quantity, financial_service=None, order_service=None):
        self.total_quantity = total_quantity
        self.sold_quantity = {}
        self.financial_service = financial_service
        self.order_service = order_service

    def place_order(self, order_id, quantity):
        print('Inventory service: starting thread to place order')
        t = threading.Thread(target=self._place_order, args=(order_id, quantity))
        t.start()
        return

    def _place_order(self, order_id, quantity):
        sleep(2)
        print('Inventory service: placing order')
        if self.total_quantity < quantity:
            print('Inventory service, failure, rolling back order')
            self.financial_service.rollback_order(order_id)
            return

        self.sold_quantity[order_id] = quantity
        self.total_quantity -= quantity
        try:
            self.order_service.complete_order(order_id)
        except:
            self.rollback_order(order_id)

    def _rollback_order(self, order_id):
        quantity = self.sold_quantity.pop(order_id)
        self.total_quantity += quantity
        self.financial_service.rollback_order(order_id)

    def rollback_order(self, order_id):
        print('Inventory service: starting thread to rollback order')
        t = threading.Thread(target=self._rollback_order, args=(order_id,))
        t.start()
        return
