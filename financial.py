import json
import time

from broker import (RabbitWrapper, FINANCIAL_ORDER_QUEUE, INVENTORY_ORDER_QUEUE, ORDER_SERVICE_RESPONSE_QUEUE,
                    FINANCIAL_ROLLBACK_QUEUE)
from database import Database
from utils import ServiceWithDowntime

ROLLED_BACK = 'rolled_back'
COMMITTED = 'committed'
PENDING = 'pending'


class InsufficientBalanceException(Exception):
    def __init__(self):
        super(InsufficientBalanceException, self).__init__('Insufficient Balance')


class TransactionNotFoundException(Exception):
    def __init__(self):
        super(TransactionNotFoundException, self).__init__('No such transaction')


class Transaction:
    def __init__(self, order_id, amount, account, _id=None):
        self.account = account
        self.amount = amount
        self.id = _id
        self.order_id = order_id
        self.status = PENDING

    def commit(self, db):
        self.account.commit_transaction(self, db)
        db.update_transaction(self.id, status=COMMITTED)

    def rollback(self, db):
        self.account.rollback_transaction(self, db)
        db.update_transaction(self.id, status=ROLLED_BACK)


class Account:
    def __init__(self, balance=0, _id=None):
        self.balance = balance
        self.id = _id
        self.transactions = {}

    def increase_balance(self, amount, db):
        new_balance = self.balance + amount
        db.update_account(self.id, balance=new_balance)

    def decrease_balance(self, amount, db):
        if self.balance > amount:
            new_balance = self.balance - amount
            db.update_account(self.id, balance=new_balance)
            return
        raise InsufficientBalanceException()

    def commit_transaction(self, transaction, db):
        self.transactions[transaction.id] = transaction
        self.decrease_balance(transaction.amount, db)

    def rollback_transaction(self, transaction, db):
        # db.delete_transaction(transaction.id)
        self.increase_balance(transaction.amount, db)


class FinancialDatabase(Database):
    class Meta:
        objs = (
            Account,
            Transaction,
        )

    def get_transaction_by_order_id(self, order_id):
        # self.read_from_storage()
        for transaction in self.transaction_storage.values():
            # print('getting transaction by order id')
            # print(transaction.order_id, order_id)
            if transaction.order_id == order_id:
                return transaction
        else:
            raise TransactionNotFoundException()


class FinancialService(ServiceWithDowntime):
    def __init__(self, db: FinancialDatabase, rabbit_interface: RabbitWrapper, downtimes):
        super(FinancialService, self).__init__(downtimes)
        self.db = db
        self.rabbit_interface = rabbit_interface

    # @use_thread
    def start_place_order_consumer(self):
        self.rabbit_interface.consume_continuously(FINANCIAL_ORDER_QUEUE, self.place_order)

    # @use_thread
    def start_rollback_order_consumer(self):
        self.rabbit_interface.consume_continuously(FINANCIAL_ROLLBACK_QUEUE, self.process_rollback_order)

    # @use_thread
    def place_order(self, ch, method, props, body):
        # print('Financial Service: placing order')
        order = json.loads(body)
        # print(f'FINANCIAL: Order time is {order["time"]}')
        # print(f'FINANCIAL: is available? {self.is_available_during(order["time"], order["time"] + 1)}')
        if not self.is_available_to_start(order['time']):
            return
        ch.basic_ack(delivery_tag=method.delivery_tag)
        # print('ack done')
        try:
            account = self.db.get_account(order['account_id'])
            transaction = self.db.create_transaction(order['order_id'], order['order_price'], account)
            # print('tran done')
            transaction.commit(self.db)
            # print(f'Financial Service: transaction committed, calling inventory, order id: {transaction.order_id}, transaction id: {transaction.id}')
            if self.is_available_until_end(order['time'], order['time'] + 1):
                self.rabbit_interface.publish(INVENTORY_ORDER_QUEUE,
                                          json.dumps(dict(order_id=order['order_id'], quantity=order['quantity'], time=order['time'])))
            # print('inv published')
            # self.inventory_service.place_order(order['order_id'], order['quantity'])
        except InsufficientBalanceException as e:
            # print('Financial service: calling inventory failed with Insufficient Balance Exception, rejecting order')
            self.rabbit_interface.publish(
                ORDER_SERVICE_RESPONSE_QUEUE.format(order_id=order['order_id']),
                json.dumps(dict(order_id=order['order_id'], status='rejected', reason=str(e))), queue_auto_delete=True)
            # self.order_service.reject_order(order['order_id'])
        # except Exception as e:
        #     print('Exception!!! #*************############*****************###########')
        #     print(e)
        #     print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
        #     print('going to rollback in financial')
        #     self.rollback_order(order['order_id'])

    # @use_thread
    def process_rollback_order(self, ch, method, props, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        message = json.loads(body)
        self.rollback_order(message['order_id'])

    # @use_thread
    def rollback_order(self, order_id):
        print(f'Financial service, rolling back order, with order id : {order_id}')
        try:
            print(self.db.transaction_storage)
            print('&&&&&&&&&&&&&&&&&&&&&&')
            for i, j in self.db.transaction_storage.items():
                print(i, vars(j))
            transaction = self.db.get_transaction_by_order_id(order_id)
            print(vars(self.db.transaction_storage[transaction.id]))
            # print(self.db.transaction_storage)
            transaction.rollback(self.db)
        except TransactionNotFoundException:
            print('Financial service: no such transaction found')
        # print('Financial service, rejecting order in order service')
        self.rabbit_interface.publish(ORDER_SERVICE_RESPONSE_QUEUE.format(order_id=order_id), json.dumps(
            dict(order_id=order_id, status='rejected', reason='Financial service rolled back order')), queue_auto_delete=True)
        # self.order_service.reject_order(order_id)

    def create_account(self, balance=100):
        return self.db.create_account(balance=balance)


def financial_starter(downtimes):
    # print('Financial starting with downtime: ', downtimes)
    financial_service = FinancialService(FinancialDatabase('fin_db'), RabbitWrapper(), downtimes)
    financial_service.create_account(1000000)
    financial_service.start_place_order_consumer()
    financial_service.start_rollback_order_consumer()
    time.sleep(4)
    print('Financial DB:')
    financial_service.db.print_objs()
    print('Financial Done')
    exit(0)