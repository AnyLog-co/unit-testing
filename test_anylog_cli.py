import os
import unittest

from rest_call import get_data
from contextlib import contextmanager

ROOT_DIR = os.path.dirname(__file__)

class TestAnyLogCommands(unittest.TestCase):
    # Class variables to be set before running tests
    query = None
    operator = None
    db_name = None

    def setUp(self):
        # Ensure required parameters are set
        assert self.query
        assert self.operator
        assert self.db_name

    @contextmanager
    def query_context(self, query:str):
        """Context manager to print query if an assertion fails."""
        try:
            yield
        except AssertionError:
            print("\n❌ Assertion failed for query:\n", query)
            raise

    def test_get_status(self):
        command = "get status where format=json"
        # Optionally add assertions
        conns = self.operator
        conns.append(self.query)

        for conn  in conns:
            results = get_data(conn, command, destination="")
            with self.query_context(command):
                self.assertIn("Status", results.json())
                data = results.json()
                assert 'running' in data.get('Status') and 'not running' not in data.get('Status')

    def test_operator_databases(self):
        command = "get databases where format=json"
        for conn in self.operator:
            result = get_data(conn, command, destination="")
            with self.query_context(command):
                self.assertIn(self.db_name, list(result.json().keys()))
                self.assertIn('almgm', list(result.json().keys()))

    def test_system_query_database(self):
        command = "get databases where format=json"
        result = get_data(self.query, command, destination="")
        with self.query_context(command):
            self.assertIn("system_query", list(result.json().keys()))

    def test_operator_processes(self):
        command = "get processes where format=json"
        for conn in self.operator:
            result = get_data(conn, command, destination="")
            with self.query_context(command):
                data = result.json()
                for key in ['TCP', 'REST', 'Operator', 'Blockchain Sync', 'Scheduler', 'Blobs Archiver']:
                    self.assertIn(key, list(data.keys()))
                    self.assertIn('Status', data.get(key))
                    self.assertIn('Running', data.get(key).get('Status'))

    def test_query_processes(self):
        command = "get processes where format=json"
        result = get_data(self.query, command, destination="")
        with self.query_context(command):
            data = result.json()
            for key in ['TCP', 'REST', 'Blockchain Sync', 'Scheduler']:
                self.assertIn(key, list(data.keys()))
                self.assertIn('Status', data.get(key))
                self.assertIn('Running', data.get(key).get('Status'))

    def test_check_tables(self):
        command = f"get data nodes where format=json and dbms={self.db_name}"
        result = get_data(self.query, command, destination="")
        data = result.json()
        with self.query_context(command):
            self.assertGreater(len(data), 0)
            for row in data:
                self.assertIn('DBMS', row)
                self.assertIn('Table', row)
                self.assertEqual(row.get('DBMS'), self.db_name)
                assert row.get('Table') in ['rand_data', 'power_plant', 'power_plant_pv']

    def test_table_columns(self):
        expected = {
            'rand_data': {
                'row_id': 'integer', 'insert_timestamp': 'timestamp without time zone', 'tsd_name': 'char(3)',
                'tsd_id': 'int', 'timestamp': 'timestamp without time zone', 'value': 'decimal'
            },
            'power_plant': {
                'row_id': 'integer', 'insert_timestamp': 'timestamp without time zone', 'tsd_name': 'char(3)',
                'tsd_id': 'int', 'monitor_id': 'char(4)', 'timestamp': 'timestamp without time zone',
                'a_n_voltage': 'int', 'a_current': 'int', 'b_n_voltage': 'int', 'realpower': 'int', 'c_current': 'int',
                'c_n_voltage': 'int', 'commsstatus': 'char(5)', 'energymultiplier': 'int', 'frequency': 'int',
                'powerfactor': 'int', 'b_current': 'int', 'reactivepower': 'int'
            },
            'power_plant_pv': {
                'row_id': 'integer', 'insert_timestamp': 'timestamp without time zone', 'tsd_name': 'char(3)',
                'tsd_id': 'int', 'monitor_id': 'character varying', 'timestamp': 'timestamp without time zone',
                'pv': 'float'
            }
        }

        # Map equivalent types across DBs/drivers
        type_equivalents = {
            'decimal': ['decimal', 'numeric', 'float', 'double precision', 'real'],
            'float': ['float', 'double precision', 'real', 'numeric', 'decimal'],
            'int': ['int', 'integer', 'bigint', 'smallint'],
            'char': ['char', 'character', 'character varying', 'varchar'],
            'timestamp without time zone': ['timestamp', 'timestamp without time zone'],
            'timestamp with time zone': ['timestamptz', 'timestamp with time zone'],
            'bool': ['boolean', 'bool'],
        }

        for table, columns in expected.items():
            command = f"get columns where dbms={self.db_name} and table={table} and format=json"
            result = get_data(self.query, command, destination="")
            actual = result.json()

            with self.query_context(command):
                for col, expected_type in columns.items():
                    actual_type = actual.get(col)
                    if not actual_type:
                        self.fail(f"Column '{col}' missing in table '{table}'")

                    expected_type_lower = expected_type.lower()
                    actual_type_lower = actual_type.lower()

                    if expected_type_lower in type_equivalents:
                        self.assertIn(
                            actual_type_lower,
                            type_equivalents[expected_type_lower],
                            msg=f"Column '{col}' in table '{table}': expected {expected_type}, got {actual_type}"
                        )
                    else:
                        # fallback to exact match if type not mapped
                        self.assertEqual(
                            actual_type_lower,
                            expected_type_lower,
                            msg=f"Column '{col}' in table '{table}': expected {expected_type}, got {actual_type}"
                        )

if __name__ == '__main__':
    # Set class variables dynamically
    TestAnyLogCommands.query = '172.23.160.85:32149'
    TestAnyLogCommands.operator = '172.23.160.85:32149'
    TestAnyLogCommands.db_name = 'new_company'

    # Use verbosity=2 for more detailed output
    unittest.main(verbosity=2)
