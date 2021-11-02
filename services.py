from financial import FinancialService, FinancialDatabase
from order import OrderService, OrderDatabase
from inventory import InventoryService

order_service = OrderService(OrderDatabase())
financial_service = FinancialService(FinancialDatabase())
inventory_service = InventoryService(1000)

order_service.financial_service = financial_service
financial_service.order_service = order_service
financial_service.inventory_service = inventory_service
inventory_service.order_service = order_service
inventory_service.financial_service = financial_service

financial_service.create_account(1000000)
