import argparse
import unittest
import sys

import rest_call
from insert_data import insert_data
from test_sql_queries import TestSQLCommands
from test_anylog_cli import TestAnyLogCommands

def anylog_test(query_conn:str, operator_conn:str, db_name:str, test_name:str, verbose:int=2):
    TestAnyLogCommands.query = query_conn
    TestAnyLogCommands.operator = operator_conn
    TestAnyLogCommands.db_name = db_name

    suite = unittest.TestLoader().loadTestsFromTestCase(TestAnyLogCommands)
    if test_name:
        suite = unittest.TestLoader().loadTestsFromName(test_name, TestAnyLogCommands)

    runner = unittest.TextTestRunner(verbosity=verbose)
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)


def sql_test(query_conn:str, db_name:str, test_name:str=None, verbose:int=2):
    TestSQLCommands.conn = query_conn
    TestSQLCommands.db_name = db_name

    suite = unittest.TestLoader().loadTestsFromTestCase(TestSQLCommands)
    if test_name:
        suite = unittest.TestLoader().loadTestsFromName(test_name,TestSQLCommands)
    runner = unittest.TextTestRunner(verbosity=verbose)
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)


def main():
    parse = argparse.ArgumentParser()
    parse.add_argument('--skip-insert', action='store_true', help="Skip data insertion")
    parse.add_argument('--skip-test', action='store_true', help="Skip running unit tests")
    parse.add_argument('--skip-query', action='store_true', help="Skip queries")
    parse.add_argument('--query', type=str, required=False, help="Query node IP:port")
    parse.add_argument('--operator', type=str, required=False, help="Comma-separated operator node IPs")
    parse.add_argument('--db-name', required=True, type=str, help="Logical database name")
    parse.add_argument('--sort-timestamps', action='store_true', help='Insert values in chronological order')
    parse.add_argument('--batch', action='store_true', help='Insert a single data batch')
    parse.add_argument('--verbose', type=int, default=2, help="Test verbosity level (0, 1, 2)")
    parse.add_argument('--select-test', type=str, default=None, help="(comma separated) specific test(s) to run")
    args = parse.parse_args()

    args.operator = args.operator.split(",")
    # insert data
    if not args.skip_insert:
        insert_data(conn=args.operator, db_name=args.db_name, sort_timestamps=args.sort_timestamp)

        for conn in args.operator.split(","):
            response = rest_call.execute_request(func='post', conn=conn, headers={'command': "flush buffers", "USer-Agent": "AnyLog/1.23"}, payload=None)
            if not 200 <= int(response.status_code) < 300:
                raise Exception(f"Failed to flush data against {conn}")

    # run query test
    if not args.skip_test:
        # run tests:
        if not args.select_test:
            anylog_test(query_conn=args.query, db_name=args.db_name)
            sql_test(query_conn=args.query, db_name=args.db_name)
        else:
            for test_case in args.select_test.strip().split(","):
                test_name = None
                if '.' in test_case:
                    test_case, test_name = test_case.split(".")

                if test_case == 'anylog':
                    anylog_test(query_conn=args.query, operator_conn=args.operator, db_name=args.db_name,
                                test_name=test_name, verbose=args.verbose)
                if test_case == "sql":
                    sql_test(query_conn=args.query, db_name=args.db_name, test_name=test_name, verbose=args.verbose)




if __name__ == '__main__':
    main()