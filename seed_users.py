# Libraries
import json
import os
import sys
from datetime import datetime
from time import time, sleep

import requests
from rich import print

from utils import *


def get_seed_users(instance_name, max_iter=10, limit=80, order='active', offset=0, store_metadata=False):
    """
    Main method for the creation of a seed set of users from a given instance
    These can be used to start a crawling process concerning the network of the instance
    instance_name: name of the instance
    max_iter: maximum number of iterations x limit of users to retrieve
    limit: number of users to retrieve for a single call
    order: order of the users to retrieve, active = most recently posted statuses (default), new = new users
    offset: offset for the users to retrieve (default is 0)
    store_metadata: whether to store the metadata of the users
    """
    try:
        # Data structures
        user_list = []
        metadata = []

        # Variables
        current_iter = 0
        condition = current_iter < max_iter

        # Setting up the url
        url = f"https://{instance_name}/api/v1/directory"

        # Preparing the headers
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}

        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold magenta]Getting a seed set of users from {instance_name}[/bold magenta]")
        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold magenta]Parameters: limit={limit}, local={local}, order={order}, store_metadata={store_metadata}[/bold magenta]")

        while condition:
            # Setting up the parameters
            params = {'limit': limit, 'local': local, 'order': order, 'offset': offset}

            # Making the request
            response = requests.get(url, headers=headers, params=params)
            
            # If the request was successful
            if response.status_code == 200:
                data = response.json()

                if len(data) == 0:
                    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{instance_name:<19}[/bold] [italic red]No more users to retrieve![/italic red]")

                # As they're local for that instance, we have to add the instance name to the user's username
                for user in data:
                    user['acct'] = user['acct'] + '@' + instance_name

                # We can now get the user list
                user_list.extend([user['acct'] for user in data])
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{instance_name:<19}[/bold] | User(s) = {len(data):>5} | current_offset = {offset:>5} | total_users = {len(user_list):>5}")


                if store_metadata:
                    metadata.extend(data)

                # Updating the condition
                current_iter += 1
                offset += limit
                condition = current_iter < max_iter

                # Checking the rate limits
                if 'X-RateLimit-Remaining' in response.headers:
                    remaining_queries = response.headers['X-RateLimit-Remaining']
                    rate_limit_reset = response.headers['X-RateLimit-Reset']
                    check_rate_limits(remaining_queries=remaining_queries, rate_limit_reset=rate_limit_reset, instance=instance_name)

            else:
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{instance_name:<19}[/bold] [italic red]Error {response.status_code} while getting the seed user(s)![/italic red]")
                condition = False

        # We exit the loop either because we reached the maximum number of iterations or because there are no more users to retrieve or because of an error
        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold magenta]Exporting {len(user_list):>5} seed set user(s) from {instance_name}![/bold magenta]")
        export_data('seed_users', instance_name, user_list)

        if store_metadata:
            export_data('seed_users', instance_name+'_metadata', metadata)

    except Exception as e:
        print(f"[{datetime.now().isoformat(timespec='seconds')}][italic red] ({e}) on line {sys.exc_info()[-1].tb_lineno} while getting the seed user(s) from {instance_name}![/italic red]")
        return None
    

if __name__ == '__main__':
    # Parameters
    instance_name = 'mastodon.social'
    max_iter = 10
    limit = 80
    order = 'active'
    offset = 0
    store_metadata = True

    # Getting the seed set of users
    get_seed_users(instance_name=instance_name, max_iter=max_iter, limit=limit, order=order, offset=offset, store_metadata=store_metadata)