import os.path
import unittest
import shutil
from rest_call import get_data

ROOT_DIR = os.path.dirname(__file__)

class TestPowerPlantData(unittest.TestCase):
    def setUp(self, conn:str='172.23.160.85:32149', db_name='new_company'):
        self.conn = conn
        self.db_name = db_name
        self.query_base = f"sql {db_name} format=json and stat=false"
        self.expect_dir = os.path.join(ROOT_DIR, 'expect')
        if not os.path.isdir(self.expect_dir):
            os.makedirs(self.expect_dir)
        self.actual_dir = os.path.join(ROOT_DIR, 'actual')
        if not os.path.isdir(self.actual_dir):
            os.makedirs(self.actual_dir)

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

        self.assertIn("Query", data)
        for row in data.get('Query'):
            table = row.get('table_name')
            row_count = row.get('row_count')
            self.assertEqual(row_count, expected_count[table])


    """
    Basic increment test against rand_data
    """
    def test_small_increments(self):
        query = f'sql {self.db_name} format=table and stat=false SELECT increments(%s, timestamp), min(timestamp)::ljust(19), max(timestamp)::ljust(19), min(value), avg(value)::float(3), max(value) FROM rand_data WHERE timestamp >= "2024-12-30 00:00:00" AND timestamp <="2025-01-02 23:59:59"'
        for increment in ['second, 1', 'second, 30', 'minute, 1', 'minute, 5', 'minute, 15', 'minute, 30',
                          'hour, 1', 'hour, 6', 'hour, 12', 'hour, 24']:
            fname = f"small_increments_{increment.strip().replace(' ', '').replace(',', '_')}.out"
            results = get_data(self.conn, query % increment)
            data = results.text
            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)
            with open(results_file, 'w') as f:
                f.write(data)
            if not os.path.isfile(expect_file):
                shutil.copy(results_file, expect_file)

            with open(results_file, "r", encoding="utf-8") as f1, open(expect_file, "r", encoding="utf-8") as f2:
                content1 = f1.read()
                content2 = f2.read()

            self.assertEqual(content1, content2, "Files do not match!")

    def test_increments(self):
        query = f'sql {self.db_name} format=table and stat=false "SELECT increments(%s, timestamp), min(timestamp)::ljust(19), max(timestamp)::ljust(19), min(value), avg(value)::float(3), max(value) FROM rand_data;"'
        for increment in ['day, 1', 'day, 7', 'day, 30', 'day, 90', 'day, 180', 'day, 365', 'year, 1']:
            fname = f"increments_{increment.strip().replace(' ', '').replace(',','_')}.out"
            results = get_data(self.conn, query % increment)
            data = results.text
            results_file = os.path.join(self.actual_dir, fname)
            expect_file = os.path.join(self.expect_dir, fname)
            with open(results_file, 'w') as f:
                f.write(data)
            if not os.path.isfile(expect_file):
                shutil.copy(results_file, expect_file)

            with open(results_file, "r", encoding="utf-8") as f1, open(expect_file, "r", encoding="utf-8") as f2:
                content1 = f1.read()
                content2 = f2.read()

            self.assertEqual(content1, content2, "Files do not match!")



if __name__ == "__main__":
    unittest.main()
