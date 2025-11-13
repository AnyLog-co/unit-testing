import datetime
import os
import random
import json
import paho.mqtt.client as mqtt

# ----------------- CONFIG -----------------
DATA_DIR = 'data'
HOST = "172.104.228.251"
PORT = 1883
USER = "anyloguser"
PASSWORD = "mqtt4AnyLog!"
ROW_COUNT = 1500
TOPICS = ['anylog-demo', 'power-plant', 'power-plant-pv']

os.makedirs(DATA_DIR, exist_ok=True)

# ----------------- TIMESTAMP GENERATION -----------------
def generate_timestamps():
    """Generate timestamps roughly every 5h49m48s from 2023-01-01 to 2025-12-31 and group by year."""
    timestamps_by_year = {2023: [], 2024: [], 2025: []}
    start_ts = datetime.datetime(2023, 1, 1, 0, 0, 0)
    end_ts = datetime.datetime(2025, 12, 31, 23, 59, 59)
    delta = datetime.timedelta(hours=5, minutes=49, seconds=48)

    current = start_ts
    while current < end_ts:
        year = current.year
        timestamps_by_year[year].append(current.strftime('%Y-%m-%dT%H:%M:%S.%f'))
        current += delta

    # Ensure last timestamp is included
    timestamps_by_year[2025].append(end_ts.strftime('%Y-%m-%dT%H:%M:%S.%f'))
    return timestamps_by_year

TIMESTAMPS_BY_YEAR = generate_timestamps()

# ----------------- GLOBAL STATE -----------------
DATA = {}

# ----------------- HELPERS -----------------
def _del_keys(row: dict, keys=('dbms', 'table')):
    for key in keys:
        row.pop(key, None)
    return row

def assign_timestamp(row_index):
    """Assign a timestamp evenly distributed per year."""
    if row_index == 0:
        return TIMESTAMPS_BY_YEAR[2023][0]
    elif row_index == ROW_COUNT - 1:
        return TIMESTAMPS_BY_YEAR[2025][-1]
    else:
        # Evenly distribute across years for intermediate rows
        # Roughly divide ROW_COUNT across 3 years
        per_year = ROW_COUNT // 3
        if row_index <= per_year:
            return random.choice(TIMESTAMPS_BY_YEAR[2023][1:])
        elif row_index <= 2 * per_year:
            return random.choice(TIMESTAMPS_BY_YEAR[2024][1:])
        else:
            return random.choice(TIMESTAMPS_BY_YEAR[2025][1:-1])

# ----------------- MQTT CALLBACKS -----------------
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    for topic in TOPICS:
        client.subscribe(topic)
        print(f"Subscribed to topic: {topic}")

def on_message(client, userdata, msg):
    file_name = os.path.join(DATA_DIR, f"data.{msg.topic.replace('-', '_')}.0.0.json")

    # Initialize counter if file not yet tracked
    if file_name not in DATA:
        DATA[file_name] = 0

    if not os.path.isfile(file_name):
        open(file_name, "w").close()

    if DATA[file_name] < ROW_COUNT:
        try:
            new_msg = json.loads(msg.payload.decode('utf-8'))
        except json.JSONDecodeError:
            print(f"Invalid JSON on topic {msg.topic}")
            return

        rows = new_msg if isinstance(new_msg, list) else [new_msg]

        for row in rows:
            if DATA[file_name] >= ROW_COUNT:
                break
            row = _del_keys(row)
            row['timestamp'] = assign_timestamp(DATA[file_name])
            DATA[file_name] += 1

            with open(file_name, 'a') as f:
                if DATA[file_name] < ROW_COUNT:
                    f.write(f"{json.dumps(row)},\n")
                else:
                    f.write(json.dumps(row))

        if DATA[file_name] % 100 == 0:
            print(file_name, DATA[file_name])

        # Disconnect when all files have enough rows
        if all(count >= ROW_COUNT for count in DATA.values()):
            print("All files reached ROW_COUNT. Disconnecting MQTT client.")
            client.disconnect()

# ----------------- MQTT CLIENT -----------------
client = mqtt.Client()
client.username_pw_set(USER, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

client.connect(HOST, PORT, 60)
client.loop_forever()
