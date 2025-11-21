import json
import time
import unittest
import source.rest_call as rest_call
from contextlib import contextmanager

DATA = [
    # full data
    {"timestamp": "2025-11-16 12:20:43.058968", "acct": "Mike",  "value1": 3,  "value2":3},
    {"timestamp": "2025-11-16 12:22:43.058968", "acct": "Bruce", "value1": 4,  "value2": 7},
    # missing value2
    {"timestamp": "2025-11-16 12:24:43.058968", "acct": "Kyle",  "value1": 6},
    # missing user
    {"timestamp": "2025-11-16 12:26:43.058968",                  "value1": 8,  "value2": 5},
    # missing timestamp
    {                                           "acct": "Don",  "value1": 3,  "value2":3}, # fails to insert when timestamp is NULL
]


def insert_data(conn:str, db_name:str):
    for row in DATA:
        rest_call.put_data(conn=conn, payload=json.dumps(row), dbms=db_name, table="t1")
        if DATA.index(row) == 1:
            rest_call.flush_buffer(conn=conn)
    rest_call.flush_buffer(conn=conn)



class TestNullData(unittest.TestCase):
    # Class variables to be set before running tests
    query = None
    operator = None
    db_name = None
    skip_insert = None

    @classmethod
    def setUpClass(self):
        # Ensure required parameters are set
        assert self.query
        assert self.operator
        assert self.db_name
        assert self.skip_insert in [True, False]

        if not self.skip_insert:
            insert_data(conn=self.operator, db_name=self.db_name)

    @contextmanager
    def query_context(self, query:str):
        """Context manager to print query if an assertion fails."""
        try:
            yield
        except AssertionError:
            print("\n❌ Assertion failed for query:\n", query)
            raise

    def test_row_count(self):
        query =  f"sql {self.db_name} format=json and stat=false select count(*) as row_count from t1"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertIn("row_count", content[0])
            self.assertEqual(content[0].get("row_count"), len(DATA))

    def test_raw_data(self):
        expected = [
            {'acct': '',      'value1': 8, 'value2': 5},
            {'acct': 'Bruce', 'value1': 4, 'value2': 7},
            {'acct': 'Don',   'value1': 3, 'value2': 3},
            {'acct': 'Kyle',  'value1': 6, 'value2': ''},
            {'acct': 'Mike',  'value1': 3, 'value2': 3}
        ]

        query =  f"sql {self.db_name} format=json and stat=false select  acct, value1, value2 from t1 order by acct"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            self.assertEqual(content, expected)


    def test_avg_values(self):
        expected = {
            'value1': 4.8,
            'value2': 4.5
        }

        query = f"sql {self.db_name} format=json and stat=false select  avg(value1) as value1, avg(value2) as value2 from t1"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            # print(content)
            self.assertEqual(content[0], expected)

    def test_values_count(self):
        expected = {'acct': 4, 'value1': 5, 'value2': 4}
        query = f"sql {self.db_name} format=json and stat=false select  count(acct) as acct, count(value1) as value1, count(value2) as value2 from t1"
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            # print(content)
            self.assertEqual(content[0], expected)

    def test_name_where(self):
        self.skipTest("Not supported AnyLog function `is`")
        query = f'sql {self.db_name} format=json and status=false select user, value1, value2 FROM t1 where user IS NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            print(content)

        query = f'sql {self.db_name} format=json and status=false select user, value1, value2 FROM t1 where user IS NOT NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            print(content)

    def test_value_where(self):
        self.skipTest("Not supported AnyLog function `is`")
        query = f'sql {self.db_name} format=json and status=false select user, value1, value2 FROM t1 where value2 IS NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            print(content)

        query = f'sql {self.db_name} format=json and status=false select user, value1, value2 FROM t1 where value2 IS NOT NULL'
        response = rest_call.get_data(conn=self.query, query=query, destination="network")
        data = response.json()
        with self.query_context(query):
            self.assertIn('Query', data)
            content = data.get('Query')
            print(content)

if __name__ == '__main__':
    # Set class variables dynamically
    TestInserts.query = '172.23.160.85:32149'
    TestInserts.operator = '172.23.160.85:32149'
    TestInserts.db_name = 'new_company'

    # Use verbosity=2 for more detailed output
    unittest.main(verbosity=2)
