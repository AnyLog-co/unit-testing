import os.path
import unittest
from rest_call import get_data
import support
from contextlib import contextmanager


ROOT_DIR = os.path.dirname(__file__)

class TestSQLCommands(unittest.TestCase):
    conn = None
    db_name = None

    def setUp(self):
        assert self.conn
        assert self.db_name

        self.query_base = f"sql {self.db_name} format=json and stat=false"

        self.expect_dir = os.path.join(ROOT_DIR, 'expect')
        support.create_dir(self.expect_dir)
        self.actual_dir = os.path.join(ROOT_DIR, 'actual')
        support.create_dir(self.actual_dir)

    @contextmanager
    def query_context(self, query:str):
        """Context manager to print query if an assertion fails."""
        try:
            yield
        except AssertionError:
            print("\n❌ Assertion failed for query:\n", query)
            raise

    """
    Get rows count for tables in network
    """
    def test_row_count_complete(self):
        expected_count = 1500 + 1500 + 100

        query = f'{self.query_base} and include=(rand_data, power_plant_pv) "SELECT COUNT(*) AS row_count FROM power_plant;"'

        results = get_data(self.conn, query)
        data = results.json()

        self.assertIn("Query", data)
        self.assertGreater(len(data["Query"]), 0)

        row_count = data["Query"][0]["row_count"]
        self.assertEqual(row_count, expected_count)

    """
    Get rows count per table in the network 
    """
    def test_row_count_per_table_complete(self):
        expected_count = {
            'rand_data': 1500,
            'power_plant_pv': 100,
            'power_plant': 1500
        }

        query = f'{self.query_base} and include=(rand_data, power_plant_pv) and extend=(@table_name) "SELECT COUNT(*) AS row_count FROM power_plant;"'

        results = get_data(self.conn, query)
        data = results.json()

        with self.query_context(query):
            self.assertIn("Query", data)

            for row in data.get('Query'):
                table = row.get('table_name')
                row_count = row.get('row_count')
                self.assertEqual(row_count, expected_count[table])

    def test_aggregations(self):
        expected = {
            'min_ts': '2023-01-01 00:00:00.000000',
            'max_ts': '2025-12-31 23:59:59.000000',
            'min_val': 0.053,
            'max_val': 974.959,
            'avg_val': 248.76575133333333,
            'row_count': 1500
        }

        query = f'{self.query_base} "SELECT min(timestamp) as min_ts, max(timestamp) as max_ts,  MIN(value) as min_val, MAX(value) as max_val, AVG(value) as avg_val, COUNT(*) as row_count FROM rand_data"'
        results = get_data(self.conn, query)
        data = results.json()

        with self.query_context(query):
            self.assertIn("Query", data)
            for row in data.get('Query'):
                for key in expected:
                    self.assertEqual(row.get(key), expected.get(key))

    def test_aggregations_group_by(self):
        expected = [
            {'monitor_id': 'BCT', 'min_ts': '2023-01-31T14:34:48', 'max_ts': '2025-12-06T18:38:24', 'row_count': 50},
            {'monitor_id': 'BF1', 'min_ts': '2023-02-07T21:28:48', 'max_ts': '2025-11-25T14:27:36', 'row_count': 50},
            {'monitor_id': 'BF2', 'min_ts': '2023-01-14T08:39:00', 'max_ts': '2025-12-29T20:29:24', 'row_count': 50},
            {'monitor_id': 'BF3', 'min_ts': '2023-01-05T03:06:36', 'max_ts': '2025-12-12T20:23:24', 'row_count': 50},
            {'monitor_id': 'BF4', 'min_ts': '2023-01-31T08:45:00', 'max_ts': '2025-12-24T23:53:24', 'row_count': 50},
            {'monitor_id': 'BG10', 'min_ts': '2023-01-01T23:19:12', 'max_ts': '2025-12-25T05:43:12', 'row_count': 50},
            {'monitor_id': 'BG11', 'min_ts': '2023-01-28T10:47:24', 'max_ts': '2025-12-22T07:45:36', 'row_count': 50},
            {'monitor_id': 'BG8', 'min_ts': '2023-02-02T13:13:12', 'max_ts': '2025-11-16T20:34:48', 'row_count': 50},
            {'monitor_id': 'BG9', 'min_ts': '2023-02-07T21:28:48', 'max_ts': '2025-12-23T07:04:48', 'row_count': 50},
            {'monitor_id': 'BSP', 'min_ts': '2023-01-17T06:36:36', 'max_ts': '2025-12-18T04:39:00', 'row_count': 52},
            {'monitor_id': 'CBT', 'min_ts': '2023-02-04T17:41:24', 'max_ts': '2025-12-04T20:00:00', 'row_count': 50},
            {'monitor_id': 'CDT', 'min_ts': '2023-01-16T07:17:24', 'max_ts': '2025-12-17T16:59:24', 'row_count': 50},
            {'monitor_id': 'CF1', 'min_ts': '2023-01-05T08:56:24', 'max_ts': '2025-12-07T23:47:24', 'row_count': 50},
            {'monitor_id': 'CF2', 'min_ts': '2023-01-07T07:34:48', 'max_ts': '2025-12-15T12:31:12', 'row_count': 50},
            {'monitor_id': 'CF3', 'min_ts': '2023-02-04T00:12:00', 'max_ts': '2025-12-24T00:34:12', 'row_count': 50},
            {'monitor_id': 'CG12', 'min_ts': '2023-01-01T17:29:24', 'max_ts': '2025-12-07T23:47:24', 'row_count': 50},
            {'monitor_id': 'CG7', 'min_ts': '2023-02-01T19:43:48', 'max_ts': '2025-12-06T12:48:36', 'row_count': 50},
            {'monitor_id': 'CSP', 'min_ts': '2023-02-13T23:13:48', 'max_ts': '2025-12-23T12:54:36', 'row_count': 50},
            {'monitor_id': 'DCT', 'min_ts': '2023-01-27T17:18:00', 'max_ts': '2025-11-26T13:46:48', 'row_count': 50},
            {'monitor_id': 'DF1', 'min_ts': '2023-01-06T02:25:48', 'max_ts': '2025-12-28T15:20:24', 'row_count': 50},
            {'monitor_id': 'DF2', 'min_ts': '2023-01-27T05:38:24', 'max_ts': '2025-12-27T21:51:00', 'row_count': 50},
            {'monitor_id': 'DF3', 'min_ts': '2023-02-02T13:13:12', 'max_ts': '2025-12-11T03:34:48', 'row_count': 50},
            {'monitor_id': 'DF4', 'min_ts': '2023-02-08T14:58:12', 'max_ts': '2025-11-08T14:21:36', 'row_count': 50},
            {'monitor_id': 'DG2', 'min_ts': '2023-03-14T21:00:00', 'max_ts': '2025-11-30T16:53:24', 'row_count': 50},
            {'monitor_id': 'DG3', 'min_ts': '2023-01-16T18:57:00', 'max_ts': '2025-12-31T23:59:59', 'row_count': 50},
            {'monitor_id': 'DG4', 'min_ts': '2023-01-20T04:34:12', 'max_ts': '2025-12-17T16:59:24', 'row_count': 49},
            {'monitor_id': 'DG5', 'min_ts': '2023-01-01T00:00:00', 'max_ts': '2025-12-24T18:03:36', 'row_count': 50},
            {'monitor_id': 'DG6', 'min_ts': '2023-02-08T09:08:24', 'max_ts': '2025-12-05T19:19:12', 'row_count': 49},
            {'monitor_id': 'DSP', 'min_ts': '2023-01-03T10:18:00', 'max_ts': '2025-12-30T02:19:12', 'row_count': 50},
            {'monitor_id': 'InconLoadTapChangerAI', 'min_ts': '2023-01-01T00:00:00', 'max_ts': '2023-12-28T05:12:36', 'row_count': 100},
            {'monitor_id': 'KPL', 'min_ts': '2023-01-12T04:10:48', 'max_ts': '2025-12-05T07:39:36', 'row_count': 50},
        ]

        query = f"{self.query_base} and include=(power_plant_pv) SELECT monitor_id, min(timestamp)::ljust(19) as min_ts, max(timestamp)::ljust(19) as max_ts, count(*) as row_count as row_count FROM power_plant GROUP BY monitor_id ORDER min_ts, monitor_id DESC"
        results = get_data(self.conn, query)
        data = results.json()

        with self.query_context(query):
            self.assertIn("Query", data)
            self.assertEqual(len(data['Query']), len(expected))
            for row in data.get('Query'):
                index = data['Query'].index(row)
                for key in expected[index]:
                    self.assertEqual(row.get(key), expected[index].get(key))

    """
    increment testing
    """
    def test_small_increments(self):
        query = f"sql {self.db_name} format=table and stat=false SELECT increments(%s, timestamp), min(timestamp)::ljust(19) as min_ts, max(timestamp)::ljust(19) as max_ts, min(value) as min_val, avg(value)::float(3) as avg_val, max(value) as max_val FROM rand_data WHERE timestamp >= '2024-12-20 00:00:00' AND timestamp <= '2025-01-10 23:59:59' ORDER BY min_ts DESC"
        for increment in ['second, 1', 'second, 30', 'minute, 1', 'minute, 5', 'minute, 15', 'minute, 30',
                          'hour, 1', 'hour, 6', 'hour, 12', 'hour, 24']:

            fname = f"small_increments_{increment.strip().replace(' ', '').replace(',', '_')}.out"
            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)

            results = get_data(self.conn, query % increment)
            data = results.text

            support.write_file(results_file, data)
            support.copy_file(results_file, expect_file)
            actual_content = support.read_file(results_file)
            expect_content = support.read_file(expect_file)

            with self.query_context(query):
                self.assertEqual(actual_content, expect_content)

    def test_increments(self):
        query = f'sql {self.db_name} format=table and stat=false "SELECT increments(%s, timestamp), min(timestamp)::ljust(19) as min_ts, max(timestamp)::ljust(19) as max_ts, min(value) as min_val, avg(value)::float(3) as avg_val, max(value) as max_val FROM rand_data ORDER BY max_ts ASC;"'
        for increment in ['day, 1', 'day, 7', 'day, 30', 'day, 90', 'day, 180', 'day, 365', 'year, 1']:

            fname = f"increments_{increment.strip().replace(' ', '').replace(',', '_')}.out"
            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)

            results = get_data(self.conn, query % increment)
            data = results.text

            support.write_file(results_file, data)
            support.copy_file(results_file, expect_file)
            actual_content = support.read_file(results_file)
            expect_content = support.read_file(expect_file)

            self.assertEqual(actual_content, expect_content)

    def test_increments_group_by(self):
        query = f"sql {self.db_name} format=table and stat=false and include=(power_plant_pv) SELECT increments(year, 1, timestamp), monitor_id, min(timestamp)::ljust(19) as min_ts, max(timestamp)::ljust(19) as max_ts, count(*) as row_count as row_count FROM power_plant GROUP BY monitor_id ORDER min_ts, monitor_id DESC"
        fname = "increments_group_by_year_1.out"

        results_file = os.path.join(self.actual_dir, fname)
        expect_file = os.path.join(self.expect_dir, fname)

        results = get_data(self.conn, query)
        data = results.text

        support.write_file(results_file, data)
        support.copy_file(results_file, expect_file)
        actual_content = support.read_file(results_file)
        expect_content = support.read_file(expect_file)

        self.assertEqual(actual_content, expect_content)

    def test_period(self):
        for period in ['minute, 1, "2023-03-12 13:42:58"', 'hour, 12, "2026-01-01 00:00:00"', 'day, 30, "2024-02-15 20:18:29"']:
            query = f"sql {self.db_name} format=table and stat=false SELECT timestamp, pv FROM power_plant_pv WHERE period({period}, timestamp) ORDER BY timestamp DESC"
            fname = f"period_{period.strip().rsplit(',',1)[0].replace(' ', '').replace(',', '_')}.out"

            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)

            results = get_data(self.conn, query)
            data = results.text

            support.write_file(results_file, data)
            support.copy_file(results_file, expect_file)
            actual_content = support.read_file(results_file)
            expect_content = support.read_file(expect_file)

            with self.query_context(query):
                self.assertEqual(actual_content, expect_content)

    def test_period_and(self):
        # first 2 cases return empty set (expected) 
        for period in ['minute, 1, "2023-03-12 13:42:58"', 'hour, 36, "2026-01-01 00:00:00"', 'day, 30, "2024-02-15 20:18:29"']:
            query = f"sql {self.db_name} format=table and stat=false SELECT timestamp, a_current, b_current, c_current FROM power_plant WHERE period({period}, timestamp) AND monitor_id='DF2' ORDER BY timestamp DESC"
            fname = f"period_and_condition_{period.strip().rsplit(',',1)[0].replace(' ', '').replace(',', '_')}.out"

            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)

            results = get_data(self.conn, query)
            data = results.text

            support.write_file(results_file, data)
            support.copy_file(results_file, expect_file)
            actual_content = support.read_file(results_file)
            expect_content = support.read_file(expect_file)

            with self.query_context(query):
                self.assertEqual(actual_content, expect_content)
            
    def test_period_complex(self):
        # first 2 cases return empty set (expected) 
        for period in ['minute, 1, "2023-03-12 13:42:58"', 'hour, 36, "2026-01-01 00:00:00"', 'day, 30, "2024-02-15 20:18:29"']:
            query = f"sql {self.db_name} format=table and stat=false SELECT monitor_id, min(timestamp) as timestamp, avg(a_current), avg(b_current) as b_current, avg(c_current) as c_current FROM power_plant WHERE period({period}, timestamp) AND (monitor_id='DF2' OR monitor_id='BSP') GROUP BY monitor_id ORDER BY timestamp, monitor_id  DESC"
            fname = f"period_complex_{period.strip().rsplit(',',1)[0].replace(' ', '').replace(',', '_')}.out"

            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)


            results = get_data(self.conn, query)
            data = results.text

            support.write_file(results_file, data)
            support.copy_file(results_file, expect_file)
            actual_content = support.read_file(results_file)
            expect_content = support.read_file(expect_file)

            with self.query_context(query):
                self.assertEqual(actual_content, expect_content)




if __name__ == "__main__":
    unittest.main(verbosity=2)
