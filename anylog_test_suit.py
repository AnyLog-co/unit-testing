import argparse
import time
import unittest
import sys

from source.insert_data import insert_data
from tests.test_sql_queries import TestSQLCommands
from tests.test_anylog_cli import TestAnyLogCommands
from tests.test_blockchain_policies import TestBlockchainPolicies
from tests.test_null_data import TestNullData
from source.rest_call import flush_buffer, get_data

def _list_methods(cls_name):
    list_methods = []
    for tname in unittest.TestLoader().loadTestsFromTestCase(cls_name):
        list_methods.append(tname._testMethodName)
    return list_methods


def _print_test_cases():
    test_cases = {
        'anylog':     _list_methods(TestAnyLogCommands),
        'blockchain': _list_methods(TestBlockchainPolicies),
        'sql':        _list_methods(TestSQLCommands),
        'null_data': _list_methods(TestNullData)
    }

    # Find the longest "key:" length (including colon)
    longest = max(len(name) + 1 for name in test_cases)  # +1 for colon

    lines = []
    for tname, tests in test_cases.items():
        key = f"{tname}:"
        padded = key.ljust(longest)  # pad after colon so lists align
        lines.append(f"\n  - {padded}  {', '.join(tests)}")

    return "".join(lines)

def _remove_skip_decorators(testcase_cls):
    # Remove decorator-based skips
    for attr_name, attr_value in list(testcase_cls.__dict__.items()):
        func = getattr(attr_value, "__func__", attr_value)  # handle bound methods
        if hasattr(func, "__unittest_skip__") and func.__unittest_skip__:
            func.__unittest_skip__ = False
            func.__unittest_skip_why__ = None

    # Patch all future instances to ignore skipTest()
    original_init = testcase_cls.__init__

    def new_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.skipTest = lambda reason=None: None  # no-op

    testcase_cls.__init__ = new_init


def _run_test(test_class_name, test_name:str=None, ignore_skip:bool=False, verbose:int=2):

    if ignore_skip or test_name:
        _remove_skip_decorators(test_class_name)

    loader = unittest.TestLoader()
    suite_all = loader.loadTestsFromTestCase(test_class_name)

    wanted = {test._testMethodName for test in suite_all}
    if test_name:
        wanted = {test for  test in test_name}

    suite = unittest.TestSuite(
        test for test in suite_all
        if test._testMethodName in wanted
    )

    runner = unittest.TextTestRunner(verbosity=verbose)
    runner.run(suite)

    sys.stdout.flush()
    time.sleep(0.5)

"""
Validate data has been insereted properly into database(s), if fails cannot continue with testing
"""
def _validate_row_count(query_conn:str, db_name:str):
    query = f"sql {db_name} format=json and stat=false and include=(power_plant, power_plant_pv) SELECT count(*) AS row_count FROM rand_data"
    is_ready = False
    index = 0

    while is_ready is False and index < 3:
        result = get_data(conn=query_conn, query=query)
        data = result.json().get('Query')[0]
        if data.get('row_count') == 3100:
            is_ready = True
        else:
            time.sleep(30)
            index += 1

    return is_ready




def anylog_test(query_conn:str, operator_conn:str, db_name:str, test_name:str, ignore_skip:bool=False, verbose:int=2):
    print("Testing related to Node status and configuration")
    sys.stdout.flush()
    time.sleep(0.5)

    TestAnyLogCommands.query = query_conn
    TestAnyLogCommands.operator = operator_conn
    TestAnyLogCommands.db_name = db_name

    _run_test(test_class_name=TestAnyLogCommands, test_name=test_name, ignore_skip=ignore_skip, verbose=verbose)


def blockchain_test(query_conn:str, is_standalone:bool=False, test_name:str=None, ignore_skip:bool=False, verbose:int=2):
    print("Testing related to blockchain policy params and relationships")
    sys.stdout.flush()
    time.sleep(0.5)

    TestBlockchainPolicies.query = query_conn
    TestBlockchainPolicies.is_standalone = is_standalone

    _run_test(test_class_name=TestBlockchainPolicies, test_name=test_name, ignore_skip=ignore_skip, verbose=verbose)


def sql_test(query_conn:str, db_name:str, test_name:str=None, ignore_skip:bool=False, verbose:int=2):
    print("Testing related to (basic) data queries")
    sys.stdout.flush()
    time.sleep(0.5)

    TestSQLCommands.conn = query_conn
    TestSQLCommands.db_name = db_name

    _run_test(test_class_name=TestSQLCommands, test_name=test_name, ignore_skip=ignore_skip, verbose=verbose)


def null_data_test(query_conn:str, operator_conn:str, db_name:str, test_name:str, skip_insert:bool=False, ignore_skip:bool=False, verbose:int=2):
    TestNullData.query = query_conn
    TestNullData.operator = operator_conn
    TestNullData.db_name = db_name
    TestNullData.skip_insert = skip_insert

    _run_test(test_class_name=TestNullData, test_name=test_name, ignore_skip=ignore_skip, verbose=verbose)

def main():
    """
    :required options:
        --query     QUERY       Query node IP:port
        --operator  OPERATOR    Comma-separated operator node IPs
        --db-name   DB_NAME     Logical database name
    :options:
        -h, --help            show this help message and exit
        --sort-timestamps   [SORT_TIMESTAMPS]   Insert values in chronological order
        --batch             [BATCH]             Insert a single data batch
        --skip-insert       [SKIP_INSERT]       Skip data insertion
        --skip-test         [SKIP_TEST]         Skip running unit tests
        --verbose           VERBOSE             Test verbosity level (0, 1, 2)
        --select-test       SELECT_TEST         (comma separated) specific test(s) to run
    """
    parse = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, epilog=f"\nList of Tests {_print_test_cases()}")
    parse.add_argument('--query',           required=False, type=str,                         default=None, help="Query node IP:port")
    parse.add_argument('--operator',        required=False, type=str,                         default=None, help="Comma-separated operator node IPs")
    parse.add_argument('--db-name',         required=False, type=str,                         default=None, help="Logical database name")
    parse.add_argument('--sort-timestamps', required=False, type=bool, nargs='?', const=True, default=False, help='Insert values in chronological order')
    parse.add_argument('--batch',           required=False, type=bool, nargs='?', const=True, default=False, help='Insert a single data batch')
    parse.add_argument('--skip-insert',     required=False, type=bool, nargs='?', const=True, default=False, help="Skip data insertion")
    parse.add_argument('--skip-test',       required=False, type=bool, nargs='?', const=True, default=False, help="Skip running unit tests")
    parse.add_argument('--verbose',         required=False, type=int,                         default=2,     help="Test verbosity level (0, 1, 2)")
    parse.add_argument('--select-test',     required=False, type=str,                         default=None, help="(comma separated) specific test(s) to run")
    parse.add_argument('--ignore-skip',     required=False, type=bool, nargs='?', const=True, default=False, help='run all tests, ignoring @unittest.skip cmd')
    parse.add_argument('--is-standalone',   required=False, type=bool, nargs='?', const=True, default=False, help="Node is a standalone instance (master, operator and query in 1 container")
    args = parse.parse_args()

    args.operator = args.operator.split(",")


    # insert data
    if not args.skip_insert:
        print("Inserting Data")
        sys.stdout.flush()
        time.sleep(0.5)
        insert_data(conns=args.operator, db_name=args.db_name, sort_timestamps=args.sort_timestamps, batch=args.batch)
        flush_buffer(conn=args.operator)

    print("Validate Data has been Insereted")
    sys.stdout.flush()
    time.sleep(0.5)
    is_ready = _validate_row_count(query_conn=args.query, db_name=args.db_name)
    if not is_ready:
        print(f"Issue with loaded data, cannot gurantee consistent results for testing, thus exiting")
        exit(1)


    # run query test
    if not args.skip_test:
        selected_tests = {}
        if args.select_test:
            for test_case in args.select_test.strip().split(","):
                test_name = None
                if '.' in test_case:
                    test_case, test_name = test_case.split(".")

                if test_case not in selected_tests:
                    selected_tests[test_case] = []
                if test_name:
                    selected_tests[test_case].append(test_name.strip())

            # for test_case in selected_tests:
            #     selected_tests[test_case] = dict(selected_tests[test_case])

            for test_case in selected_tests:
                if test_case == 'anylog':
                    anylog_test(query_conn=args.query, operator_conn=args.operator, db_name=args.db_name, test_name=selected_tests[test_case], ignore_skip=args.ignore_skip, verbose=args.verbose)
                if test_case == 'blockchain':
                    blockchain_test(query_conn=args.query, is_standalone=args.is_standalone, test_name=selected_tests[test_case], ignore_skip=args.ignore_skip, verbose=args.verbose)
                if test_case == "sql":
                    sql_test(query_conn=args.query, db_name=args.db_name, test_name=selected_tests[test_case], ignore_skip=args.ignore_skip, verbose=args.verbose)
        else:
            anylog_test(query_conn=args.query, operator_conn=args.operator, db_name=args.db_name, test_name=args.select_test, ignore_skip=args.ignore_skip, verbose=args.verbose)
            blockchain_test(query_conn=args.query, is_standalone=args.is_standalone, test_name=args.select_test, ignore_skip=args.ignore_skip, verbose=args.verbose)
            sql_test(query_conn=args.query, db_name=args.db_name, test_name=args.select_test, ignore_skip=args.ignore_skip, verbose=args.verbose)






if __name__ == '__main__':
    main()