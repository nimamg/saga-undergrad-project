import random
from pprint import pprint

import yaml


def get_methods_from_spec(spec_file_name):
    with open(spec_file_name, 'r') as spec_file:
        spec = yaml.safe_load(spec_file)
        methods = []
        for service in spec['services']:
            for method in service['methods']:
                method['service'] = service['name']
                methods.append(method)
        return methods


def generate_test_cases(methods):
    test_cases = []
    for method in methods:
        if method['role'] == 'initiator':
            publishes = method['publishes']['name']
            time_spent = 2
            break
    else:
        raise Exception
    while True:
        for method in methods:
            if method['role'] == 'initiator' and method['consumes'] == publishes:
                break
            if method['role'] == 'participant' and method['trigger'] == publishes:
                request_time = random.randint(1, 10) + time_spent
                # Service Down before getting event
                test_cases.append({
                    'request_time': request_time,
                    'test_case_name': f'{method["service"]} {method["name"]} before event',
                    'downtimes': [{
                        'interval': {
                            'start': max(request_time - random.randint(1, 4), 1),
                            'end': request_time + random.randint(3, 5)
                        },
                        'service': method['service'],
                    }]
                })
                # Service Down while processing event
                request_time = random.randint(1, 10) + time_spent
                test_cases.append({
                    'request_time': request_time,
                    'test_case_name': f'{method["service"]} {method["name"]} down during event',
                    'downtimes': [{
                        'interval': {
                            'start': request_time + 1,
                            'end': request_time + random.randint(4, 7)
                        },
                        'service': method['service'],
                    }]
                })
                time_spent += 2
                publishes = method['publishes']['name']
        else:
            continue
        break
    test_cases.append({
        'request_time': random.randint(1, 10),
        'test_case_name': 'Happy Path',
        'downtimes': []
    })
    return {'test_cases': test_cases}


with open('test_case.yaml', 'w') as test_case_file:
    test_cases = generate_test_cases(get_methods_from_spec('spec.yml'))
    pprint(test_cases)
    yaml.dump(test_cases, test_case_file)
