import time
import requests

def execute_request(func:str, conn:str, headers:dict, payload:str=None):
    try:
        if func.upper() == 'GET':
            response = requests.get(url=f"http://{conn}", headers=headers)
        elif func.upper() == 'PUT':
            response = requests.put(url=f"http://{conn}", headers=headers, data=payload)
        elif func.upper() == 'POST':
            response = requests.post(url=f"http://{conn}", headers=headers, data=payload)
        else:
            raise ValueError(f'Invalid user input {func.upper()}')
        response.raise_for_status()
    except Exception as error:
        raise Exception(f"Failed to execute {func.upper()} against {conn} (Error;  {error})")
    return response


def put_data(conn:str, payload:str, dbms:str, table:str):
    headers = {
        'type': 'json',
        'dbms': dbms,
        'table': table,
        'mode': 'streaming',
        'Content-Type': 'text/plain'
    }

    execute_request(func='PUT', conn=conn, headers=headers, payload=payload)


def get_data(conn:str, query:str, destination:str='network'):
    headers = {
        'command': query,
        'User-Agent': 'AnyLog/1.23',
    }
    if destination:
        headers['destination'] = destination

    return execute_request(func='GET', conn=conn, headers=headers, payload=None)


def flush_buffer(conn:(str or list)):
    """
    Code to flush insert data buffers
    """
    headers = {"command": "flush buffers", "User-Agent": "AnyLog/1.23"}
    if isinstance(conn, str):
        execute_request(func='POST', conn=conn, headers=headers, payload=None)
    else:
        for con in conn:
            execute_request(func='POST', conn=con, headers=headers, payload=None)
    time.sleep(5)

