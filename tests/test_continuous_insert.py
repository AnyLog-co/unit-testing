import unittest
import threading
import time
import random
import datetime
import json
import copy
import logging
from typing import Any, Dict, List, Optional

"""
This unittest simulates:
 - multiple insert threads
 - multiple query threads
 - 2-minute run
 - final verification of COUNT, summary, aggregates
"""

# ----------------------------------------------------------
# Try importing real API; fallback to mocks for local testing
# ----------------------------------------------------------

try:
    from source.rest_call import put_data, flush_buffer, get_data
except Exception:
    logging.warning("Using internal MOCKS (source.rest_call not found)")

    _MOCK_DB_LOCK = threading.Lock()
    _MOCK_DB: Dict[str, List[Dict[str, Any]]] = {}

    def put_data(conn: str, dbms: str, table: str, payload: str):
        row = json.loads(payload)
        with _MOCK_DB_LOCK:
            _MOCK_DB.setdefault(conn, []).append(row)
        class R:
            status_code = 200
            def json(self): return {"ok": True}
        return R()

    def flush_buffer(conn: Optional[str]):
        class R:
            status_code = 200
            def json(self): return {"flushed": True}
        return R()

    def get_data(conn: str, query: str):
        with _MOCK_DB_LOCK:
            rows = list(_MOCK_DB.get(conn, []))
        q = query.lower()

        if "count(" in q:
            result = [{"row_count": len(rows)}]

        elif "min(timestamp)" in q and "max(timestamp)" in q:
            if not rows:
                result = [{"min_ts": None, "max_ts": None, "count": 0}]
            else:
                ts_list = [r["timestamp"] for r in rows]
                result = [{
                    "min_ts": min(ts_list),
                    "max_ts": max(ts_list),
                    "count": len(rows)
                }]

        elif "min(value)" in q and "avg(value)" in q:
            if not rows:
                result = [{"min_val": None, "max_val": None, "avg_val": None}]
            else:
                vals = [float(r["value"]) for r in rows]
                result = [{
                    "min_val": min(vals),
                    "max_val": max(vals),
                    "avg_val": sum(vals)/len(vals)
                }]

        else:
            result = [{"rows": rows}]

        class R:
            status_code = 200
            def json(self): return result
        return R()


# ----------------------------------------------------------
# Shared state for test
# ----------------------------------------------------------

DATA: List[Dict[str, Any]] = []
DATA_LOCK = threading.Lock()
STOP_EVENT = threading.Event()
TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S.%f"


def generate_row() -> Dict[str, Any]:
    uname = f"User{random.choice([1, 2, 3, 4, 5])}"
    ts = datetime.datetime.now()
    value = (
        round(random.random() * 100, random.randint(0, 9))
        if random.choice([True, False])
        else random.randint(1, 100)
    )
    return {
        "uname": uname,
        "timestamp": ts.strftime(TIMESTAMP_FMT),
        "value": value,
    }


def safe_json_dumps(row):
    return json.dumps(row)


# ----------------------------------------------------------
# Insert Worker
# ----------------------------------------------------------

def insert_worker(conns, db_name, table, max_sleep):
    while not STOP_EVENT.is_set():
        conn = random.choice(conns)
        pre_conn = None

        row = generate_row()
        payload = safe_json_dumps(row)

        try:
            put_data(conn=conn, dbms=db_name, table=table, payload=payload)
            with DATA_LOCK:
                DATA.append(copy.deepcopy(row))
        except Exception as e:
            logging.exception("put_data failed on %s: %s", conn, e)

        if len(conns) > 1:
            pre_conn = conn
            tries = 0
            while True:
                next_conn = random.choice(conns)
                if next_conn != pre_conn or tries > 10:
                    conn = next_conn
                    break
                tries += 1

        # flush ~33% chance
        if random.randint(1, 100) % 3 == 0:
            try:
                flush_buffer(conn)
                if pre_conn:
                    flush_buffer(pre_conn)
            except Exception as e:
                logging.exception("flush_buffer failed: %s", e)

        time.sleep(random.random() * max_sleep)


# ----------------------------------------------------------
# Query Worker
# ----------------------------------------------------------

def parse_count_response(r):
    if isinstance(r, list) and r:
        r = r[0]
    if isinstance(r, dict):
        return int(r.get("row_count") or r.get("count") or 0)
    return 0


def parse_summary(r):
    if isinstance(r, list) and r:
        r = r[0]
    return {
        "min_ts": r.get("min_ts"),
        "max_ts": r.get("max_ts"),
        "count": int(r.get("count", 0)),
    }


def parse_agg(r):
    if isinstance(r, list) and r:
        r = r[0]
    def f(x):
        try: return float(x)
        except: return None
    return {
        "min_val": f(r.get("min_val")),
        "max_val": f(r.get("max_val")),
        "avg_val": f(r.get("avg_val")),
    }


def query_worker(conns, db_name, table, sleep_choices):
    queries = {
        "count":
            f"SELECT COUNT(*) AS row_count FROM {table}",
        "summary":
            f"SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts, COUNT(*) as count FROM {table}",
        "aggregates":
            f"SELECT MIN(value) as min_val, MAX(value) AS max_val, AVG(value) AS avg_val FROM {table}",
    }

    while not STOP_EVENT.is_set():
        conn = random.choice(conns)
        qname = random.choice(list(queries))
        sql = f"sql {db_name} format=json and stat=false {queries[qname]}"

        try:
            resp = get_data(conn, sql).json()
            # Logging only lightly
            if qname == "count":
                _ = parse_count_response(resp)
            elif qname == "summary":
                _ = parse_summary(resp)
            else:
                _ = parse_agg(resp)
        except Exception as e:
            logging.exception("query failed: %s", e)

        sleep_time = random.choice(sleep_choices)
        for _ in range(sleep_time):
            if STOP_EVENT.is_set():
                break
            time.sleep(1)


# ----------------------------------------------------------
# Final Verification
# ----------------------------------------------------------

def final_verification(conns, db_name, table):
    for c in conns:
        try:
            flush_buffer(c)
        except Exception:
            pass

    with DATA_LOCK:
        snapshot = copy.deepcopy(DATA)

    expected_count = len(snapshot)
    expected_vals = [float(r["value"]) for r in snapshot] if snapshot else []
    expected_min_ts = min([r["timestamp"] for r in snapshot]) if snapshot else None
    expected_max_ts = max([r["timestamp"] for r in snapshot]) if snapshot else None

    verify_conn = conns[0]

    def run(q):
        full = f"sql {db_name} format=json and stat=false {q}"
        return get_data(verify_conn, full).json()

    # Count
    db_count = parse_count_response(
        run(f"SELECT COUNT(*) AS row_count FROM {table}")
    )
    assert db_count == expected_count, f"COUNT mismatch: {db_count} vs {expected_count}"

    # Summary
    summary = parse_summary(
        run(f"SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM {table}")
    )
    assert summary["count"] == expected_count

    if expected_min_ts and summary["min_ts"]:
        assert summary["min_ts"] == expected_min_ts
    if expected_max_ts and summary["max_ts"]:
        assert summary["max_ts"] == expected_max_ts

    # Aggregates
    if expected_vals:
        db_agg = parse_agg(
            run(f"SELECT MIN(value), MAX(value), AVG(value) FROM {table}")
        )
        eps = 1e-3

        assert abs(db_agg["min_val"] - min(expected_vals)) < eps
        assert abs(db_agg["max_val"] - max(expected_vals)) < eps
        assert abs(db_agg["avg_val"] - (sum(expected_vals)/len(expected_vals))) < eps


# ----------------------------------------------------------
# The Test Case
# ----------------------------------------------------------

class TestContinuousLoad(unittest.TestCase):

    def test_continuous_load(self):
        """
        Runs:
          - 2 insert workers
          - 2 query workers
          - 2 minutes
        Then flushes and asserts DB matches memory
        """
        conns = ["conn1", "conn2"]
        db = "testdb"
        table = "continuous_data"

        STOP_EVENT.clear()

        insert_threads = []
        query_threads = []

        # Start insert workers
        for i in range(2):
            t = threading.Thread(
                target=insert_worker,
                args=(conns, db, table, 5.0),   # max insert sleep
                daemon=True,
            )
            t.start()
            insert_threads.append(t)

        # Start query workers
        for i in range(2):
            t = threading.Thread(
                target=query_worker,
                args=(conns, db, table, [5, 10, 15, 25]),  # your new sleep choices
                daemon=True,
            )
            t.start()
            query_threads.append(t)

        # ---- Run for 2 minutes ----
        runtime = 2 * 60
        end_ts = time.time() + runtime

        while time.time() < end_ts:
            time.sleep(1)

        # Signal stop
        STOP_EVENT.set()

        # Join workers
        for t in insert_threads + query_threads:
            t.join(timeout=3)

        # ---- Verification ----
        final_verification(conns, db, table)


if __name__ == "__main__":
    unittest.main()

#------- Original -----
import datetime
import json
import random
import time
from source.rest_call import put_data,flush_buffer, get_data

DATA = []


def generate_row():
    """
    Generate new row to be inserted
    """
    global DATA
    uname = f"User{random.choice([1, 2, 3 ,4, 5])}"
    timestamp = datetime.datetime.now()
    value = random.choice([round(random.random() * 100, random.choice(list(range(10)))), random.randint(1, 100)])

    row = {"uname": uname, "timestamp": timestamp, "value": value}
    DATA.append(row)
    row['timestamp'] = row['timestamp'].strftime('%Y-%m-%d %H:$M:$S.$f')

    return json.dumps(row)

def insert_data(conns:list, db_name:str):
    """
    Actual process to insert data (as a thread)
    :todo:
        add while and have it run for 5 minutes
        in main configure as a thread
    """
    conn = random.choice(conns)
    pre_conn = None

    row = generate_row()
    put_data(conn=conn, dbms=db_name, table="continuous_data", payload=row)

    if len(conns) > 1:
        while pre_conn is None or pre_conn == conn:
            pre_conn = conn
            conn = random.choice(conns)

    if random.randint(1, 100) % 3 == 0: # this is lise `commit` in PSQL
        flush_buffer(conn=conn)
        flush_buffer(conn=pre_conn)

    time.sleep(random.choice([x * 0.25 for x in range(int(5 / 0.25) + 1)]))


def query_data(conn:str, db_name:str):
    """
    Randomly execute queries against the network
    :todo:
        1. integrate assertion
        2. when `insert_data` has stopped  then do the following:
            - flush the buffers
            - run all 3 queries and assert they get the same results as DATA
    """
    global DATA
    queries = {
        "row count": "SELECT COUNT(*) AS row_count FROM continuous_data",
        "summary": "SELECT MIN(timestamp) as min_ts, MAX(TIMESTAMP) as max_ts, COUNT(*) FROM continuous_data",
        "aggregates": "SELECT MIN(value) as min_val, MAX(value) AS max_val, AVG(value) AS avg_val  FROM continuous_data",
    }

    while True:
        query_title = random.choice(list(queries))
        query = f"sql {db_name} format=json and stat=false {queries[query_title]}"
        with subTest(query):
            results = get_data(conn, query)
            data = results.json()

        time.sleep(random.choice[25, 30, 50, 60, 75, 90])