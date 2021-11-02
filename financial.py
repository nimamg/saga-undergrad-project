from time import sleep

from database import Database
import threading

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

    def commit(self):
        self.account.commit_transaction(self)
        self.status = COMMITTED

    def rollback(self):
        self.account.rollback_transaction(self)
        self.status = ROLLED_BACK


class Account:
    def __init__(self, balance=0, _id=None):
        self.balance = balance
        self.id = _id
        self.transactions = {}

    def increase_balance(self, amount):
        self.balance += amount

    def decrease_balance(self, amount):
        if self.balance > amount:
            self.balance -= amount
            return
        raise InsufficientBalanceException()

    def commit_transaction(self, transaction):
        self.decrease_balance(transaction.amount)
        self.transactions[transaction.id] = transaction

    def rollback_transaction(self, transaction):
        try:
            self.transactions.pop(transaction.id)
            self.increase_balance(transaction.amount)
        except KeyError:
            pass


class FinancialDatabase(Database):

    class Meta:
        objs = (
            Account,
            Transaction,
        )

    def get_transaction_by_order_id(self, order_id):
        for transaction in self.transaction_storage.values():
            if transaction.order_id == order_id:
                return transaction
        else:
            raise TransactionNotFoundException()


class FinancialService:
    def __init__(self, db, order_service=None, inventory_service=None):
        self.db = db
        self.order_service = order_service
        self.inventory_service = inventory_service

    def place_order(self, order_id, amount, account_id, quantity):
        print('Financial service: starting thread')
        t = threading.Thread(target=self._place_order, args=(order_id, amount, account_id, quantity))
        t.start()
        return

    def _place_order(self, order_id, amount, account_id, quantity):
        sleep(2)
        print('Financial Service: placing order')
        try:
            transaction = self.db.create_transaction(order_id, amount, self.db.get_account(account_id))
            transaction.commit()
            print('Financial Service: transaction committed, calling inventory')
            self.inventory_service.place_order(order_id, quantity)
        except InsufficientBalanceException as e:
            print('Financial service: calling inventory failed with Insufficient Balance Exception, rejecting order')
            self.order_service.reject_order(order_id)

    def _rollback_order(self, order_id):
        print('Financial service, rolling back order')
        try:
            transaction = self.db.get_transaction_by_order_id(order_id)
            transaction.rollback()
        except TransactionNotFoundException:
            print('Financial service: no such transaction found')
        print('Financial service, rejecting order in order service')
        self.order_service.reject_order(order_id)

    def rollback_order(self, order_id):
        print('Financial service: starting thread to rollback order')
        t = threading.Thread(target=self._rollback_order, args=(order_id,))
        t.start()
        return

    def create_account(self, balance=100):
        return self.db.create_account(balance=balance)
