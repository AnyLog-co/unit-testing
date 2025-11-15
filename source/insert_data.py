import argparse
import json
import os
import datetime
import random

from source.rest_call import put_data

CONNS = []
LAST_CONN = None
ROOT_DIR = os.path.dirname(__file__).rsplit('source', 1)[0]
DATA_FILES = [os.path.join(ROOT_DIR, 'data', fname) for fname in os.listdir(os.path.join(ROOT_DIR, 'data')) if fname.endswith('json')]

def _sort_data(records:list)->list:
    for i in range(len(records)):
        records[i]['timestamp'] = datetime.datetime.strptime(records[i]['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")

    records.sort(key=lambda item: item["timestamp"])

    for i in range(len(records)):
        records[i]['timestamp'] = records[i]['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    return records



def insert_data(conn:list, db_name:str, sort_timestamps:bool=False, batch:bool=False):
    global CONNS
    global LAST_CONN
    CONNS = conn

    for fname in DATA_FILES:
        if not os.path.isfile(fname):
            raise FileNotFoundError(f"File {fname} not found")

        payload = []
        if not db_name:
            db_name, table, *_ = fname.split(".")
        else:
            _, table, *_ = fname.split(".")

        with open (fname, 'r') as f:
            for line in f:
                line = line.strip()     # remove whitespace at both ends
                line = line.rstrip(",") # remove any trailing comma
                if line:
                    try:
                        payload.append(json.loads(line))
                    except:
                        print(line)

        if sort_timestamps:
            payload = _sort_data(payload)

        if payload:
            conn = random.choice(CONNS)
            if batch is True:
                serialized_payload = json.dumps(payload)
                put_data(conn=conn, dbms=db_name, table=table, payload=serialized_payload)
            else:
                for row in payload:
                    if LAST_CONN and len(CONNS) > 1:
                        while conn == LAST_CONN:
                            conn = random.choice(CONNS)
                    serialized_payload = json.dumps(row)
                    put_data(conn=conn, dbms=db_name, table=table, payload=serialized_payload)
                    LAST_CONN = conn

if __name__ == '__main__':
    parse = argparse.ArgumentParser()
    parse.add_argument('conn', type=str, default='127.0.0.1:32149', help='REST conn for operator node')
    parse.add_argument('--db-name', type=str, default=None, help='logical database name')
    parse.add_argument('--sort-timestamps', type=bool, nargs='?', const=True, default=False,
                       help='Insert values chronological order')
    parse.add_argument('--batch', type=bool, nargs='?', const=True, default=False, help='Insert a single data in batch')
    args = parse.parse_args()

    insert_data(conn=args.conn.split(","), db_name=args.db_name, sort_timestamps=args.sort_Timestmaps, batch=args.batch)
