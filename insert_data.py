import argparse
import json
import os
import datetime
from rest_call import put_data

CONNS = []
LAST_CONN = None
DATA_FILES = [os.path.join('data', fname) for fname in os.listdir('data')]

def _sort_data(records:list)->list:
    for i in range(len(records)):
        records[i]['timestamp'] = datetime.datetime.strptime(records[i]['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")

    records.sort(key=lambda item: item["timestamp"])

    for i in range(len(records)):
        records[i]['timestamp'] = records[i]['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    return records


def main():
    global CONNS
    parse = argparse.ArgumentParser()
    parse.add_argument('conn', type=str, default='127.0.0.1:32149', help='REST conn for operator node')
    parse.add_argument('--db-name', type=str, default=None, help='logical database name')
    parse.add_argument('--sort', type=bool, nargs='?', const=True, default=False, help='Insert values chronological order')
    parse.add_argument('--batch', type=bool, nargs='?', const=True, default=False, help='Insert a single data in batch')
    args = parse.parse_args()

    CONNS = args.conn.split(",")

    for fname in DATA_FILES:
        if not os.path.isfile(fname):
            raise FileNotFoundError(f"File {fname} not found")

        payload = []
        db_name, table, *_ = fname.split(".")
        if args.db_name:
            db_name = args.db_name

        with open (fname, 'r') as f:
            for line in f:
                if line.strip():
                    payload.append(json.loads(line.strip().rsplit(",",1)[0] if line.strip().endswith(",") else line.strip()))

        if args.sort:
            payload = _sort_data(payload)

        if payload:
            put_data(conn=args.conn, dbms=db_name, table=table, payload=payload, batch=args.batch)

if __name__ == '__main__':
    main()
