# Libraries
from datetime import datetime, timezone
from time import sleep
from rich import print
import json
import os
import sys


def get_waiting_time(t):
    """
    Gets the waiting time w.r.t. the current rate limit reset time
    t: current reset time
    """
    # Time provided by Mastodon is Zulu time
    refresh_time = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
    current_time = datetime.now(timezone.utc)

    # Current time has timezone info, unlike refresh_time
    current_time = current_time.replace(tzinfo=None)

    if refresh_time > current_time:
        waiting_time = refresh_time - current_time
        return waiting_time.seconds
    else:
        return 0


def check_rate_limits(remaining_queries, rate_limit_reset, instance, tolerance=5):
    """
    Checking if rate limit is approaching
    remaining_queries: number of queries to the rate limit
    rate_limit_reset: reset time for the next rate limit
    instance: server where the rate limit applies
    id: current thread
    tolerance: tolerance w.r.t. the number of remaining queries (which corresponds to the number of threads or a fixed number)
    """
    # No try catch here, if it fails, we should consider it as unprocessed and process it later
    if int(remaining_queries) <= int(tolerance):  # a bit of trade-off, to avoid getting banned
        waiting_time = get_waiting_time(rate_limit_reset)
        if waiting_time > 0:
            print(f"[{datetime.now()}][bold magenta] Rate limit reached for {instance} waiting for {waiting_time} sec.[/bold magenta]")
            sleep(waiting_time)
            print(f"[{datetime.now()}][bold magenta] Rate limit ok for {instance}![/bold magenta]")


def previous_data(operation, instance_name):
    """
    Check if there is any previous data for the instance
    instance_name: name of the instance
    """
    try:
        if not os.path.exists(f'__mastodonte__/{operation}_processed.json'):
            return None
        
        with open(f'__mastodonte__/{operation}_processed.json', 'r') as _f:
            processed_instances = json.load(_f)
            instance_metadata = processed_instances.get(instance_name, None)
            if instance_metadata is not None:
                return instance_metadata
            else:
                return None
    except Exception as e:
        print(f"[{datetime.now().isoformat(timespec='seconds')}][italic red] ({e}) on line {sys.exc_info()[-1].tb_lineno} while checking previous data for {instance_name}![/italic red]")
        return None



def export_data(operation, filename, data):
    """
    Saving the results for instance_name into a JSON
    filename: name of the file
    data: content to be saved
    """

    # Checking if there is already data for instance_name
    try:
        # If the operation folder does not exist, create it
        os.makedirs(operation, exist_ok=True)
        with open(f'{operation}/{filename}.json', 'r') as _f:
            _data = json.load(_f)
            to_save = _data + data

        with open(f'{operation}/{filename}.json', 'w') as _f:
            json.dump(to_save, _f, indent=4)
            
    except FileNotFoundError:
        # No data for instance_name
        # We can directly save the data
        with open(f'{operation}/{filename}.json', 'w') as _f:
            json.dump(data, _f, indent=4)



def export_user_network(operation, filename, data):
    """
    Saving the results for the user network into a JSON
    filename: name of the file
    data: content to be saved
    """

    try:
        # If the operation folder does not exist, create it
        os.makedirs(operation, exist_ok=True)
        with open(f'{operation}/{filename}.json', 'w') as _f:
            json.dump(data, _f, indent=4)
    except Exception as e:
        raise e