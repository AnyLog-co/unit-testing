# AnyLog Unit Testing 

The following provides a platform to easy add and execute testing against AnyLog / EdgeLake. 

The goal is whenever we encounter a bug, we add more data and test cases to be executed.

The test does the following steps: 
1. Insert data 
2. [test_anylog_cli.py](test_anylog_cli.py)   - execute basic `get` commands that validate connectivity, configurations and that the data exists 
3. [test_sql_queries.py](test_sql_queries.py) - execute an array of common `SELECT` statements - expected results were do against a table without partitioning
   * aggregations
   * increments 
   * group by 
   * where 
   * period 
   * raw data

**Missing**: 
1. automatically deploying a small network  
   * 1 docker container that has everything 
   * Master, operator, query 
   * Master, 2 operator, query
   * Master, 2 operator (HA), query
   * Master, 3 operators (2 HA), query 
2. Insert data using POST and MQTT 
3. Remove data and associated blockchain policies from network 
4. teardown the entire network (used for overnight / testing) if everything passed 
5. store summary to file(s)


## Updating Setup

### Adding New Data 
Add JSON file(s) to [data](data) directory - [insert_data.py](insert_data.py) will automatically grab the JSON file(s) 
and publish them to the operator node(s).

### Option 1
To implement a new function in an exiting file simply add the test with the keyword "test_" in front of it. 

For example, lets say I want to add a test that gets a row count and the length of time data has been inserting.

```python
# file: test_sql_queries.py

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
    # ... older tests ... 
    def test_timediff_count(self):
        query = f"{self.query_base} SELECT min(timestamp) as min, max(timestamp) as max, min(timestamp)::timediff(max(timestamp)) as diff, count(*) as count FROM pp_pm"
        expected = {
           "min": "2025-01-12T08:15:22Z",
           "max": "2025-01-12T14:47:09Z",
           "timediff": "06:31:47",
           "count": 4821
        }
        
        results = get_data(self.conn, query)
        data = results.json()

        with self.query_context(query):
            self.assertIn("Query", data)
            self.assertEqual(data['Query'][0], expected)
```

### Option 2
Implement a new test program all together

**Phase 1**: Creating a new file 
1. Copy an exiting test class into your repository
2. Remove all `test_` content
3. Update `setUp()` with needed params 
4. Create your own `test_` test cases 

**Phase 2**: Update [anylog_test_suit.py](anylog_test_suit.py)
1. Import the new class 
2. Update [`_print_test_cases`](anylog_test_suit.py#L17) method with new nickname and call to generate list of tests
3. Above `main`, add a new function that calls the new unittest 
4. Update `main` to call the new unittests under `if not args.skip_test`. 
