from broker import RabbitWrapper
from financial import FinancialService, FinancialDatabase
from order import OrderService, OrderDatabase
from inventory import InventoryService, InventoryDatabase

order_service = OrderService(OrderDatabase(), RabbitWrapper())
financial_service = FinancialService(FinancialDatabase(), RabbitWrapper())
inventory_service = InventoryService(InventoryDatabase(), RabbitWrapper())

# financial_service.start_place_order_consumer()
# financial_service.start_rollback_order_consumer()
# inventory_service.start_place_order_consumer()
# inventory_service.start_rollback_order_consumer()


financial_service.create_account(1000000)
