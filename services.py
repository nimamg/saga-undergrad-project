# from broker import RabbitWrapper
# from financial import FinancialService, FinancialDatabase
# from order import OrderService, OrderDatabase
# from inventory import InventoryService, InventoryDatabase
#
# order_service = OrderService(OrderDatabase('order_db'), RabbitWrapper())
# financial_service = FinancialService(FinancialDatabase('fin_db'), RabbitWrapper())
# inventory_service = InventoryService(InventoryDatabase('inv_db'), RabbitWrapper())

# financial_service.start_place_order_consumer()
# financial_service.start_rollback_order_consumer()
# inventory_service.start_place_order_consumer()
# inventory_service.start_rollback_order_consumer()


# financial_service.create_account(1000000)

from multiprocessing import Process

from broker import delete_queues
from order import order_starter
from financial import financial_starter
from inventory import inventory_starter
import yaml

with open('test_case.yaml', 'r') as f:
    test_cases = yaml.safe_load(f)['test_cases']

service_starters = {
    'OrderService': order_starter,
    'FinancialService': financial_starter,
    'InventoryService': inventory_starter
}

processes = {}
for test_case in test_cases:
    print('Starting test case: {}'.format(test_case['test_case_name']))
    for service in service_starters:
        # print('iterating service {}'.format(service))
        for d in test_case['downtimes']:
            if service == d['service']:
                downtime = d['interval']
                break
        else:
            downtime = {}
        if service == 'OrderService':
            args = [100, 1, test_case['request_time']]
        else:
            args = [[]] if not downtime else [[downtime]]
        print('starting {} with args {}'.format(service, args))
        processes[service] = Process(target=service_starters[service], args=args)
        processes[service].start()
    print('all services started')
    for service, process in processes.items():
        # print('waiting for {}'.format(service))
        process.join()
    delete_queues()
    input('Press any key to continue')
    print('######################################################')
