# Libraries
import json
import os
import sys
from datetime import datetime
from time import time, sleep

import requests
from rich import print

from utils import *


def get_user_followers(username, limit=80, store_metadata=False):
    """
    Method for getting the list of followers of a user
    user: user who we want to get the followers
    limit: number of followers to retrieve for a single call
    store_metadata: whether to store the metadata of the followers
    """

    try:
        # Data structures
        follower_list = []
        metadata = []
        iter = 1

        # Preparing the headers
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}

        # Getting the account info
        acct, instance_name = username.split('@')
        user_id = None

        # Looking up for the user's id
        lookup_url = f"https://{instance_name}/api/v1/accounts/lookup?acct={acct}"
        lookup_response = requests.get(lookup_url, headers=headers)

        # Getting the user id
        if lookup_response.status_code == 200:
            lookup_page = lookup_response.json()
            if len(lookup_page) == 0:
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] [italic red]User not found![/italic red]")
                return None
            
            user_id = lookup_page['id']
            assert acct == lookup_page['acct'], f"Error: {acct} != {lookup_page['acct']}"
            print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] has ID = {user_id}")

        # We start with a valid condition
        condition = user_id is not None

        # Setting up the url
        url = f"https://{instance_name}/api/v1/accounts/{user_id}/followers" 

        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold magenta]Crawling the {username}'s followers[/bold magenta]")

        while condition:
            # Setting up the parameters
            params = {'limit': limit}

            # Making the request
            response = requests.get(url, headers=headers, params=params)
            
            # If the request was successful
            if response.status_code == 200:
                data = response.json()

                if len(data) == 0:
                    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] [italic red]No more followers to retrieve![/italic red]")

                # If we find local users, we have to add the instance name to the user's username
                for user in data:
                    if '@' not in user['acct']:
                        user['acct'] = user['acct'] + '@' + instance_name

                # We can now get the user list
                follower_list.extend([user['acct'] for user in data])
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] | Follower(s) = {len(data):>5} | total_followers = {len(follower_list):>5}")


                if store_metadata:
                    metadata.extend(data)

                # Checking the rate limits
                if 'X-RateLimit-Remaining' in response.headers:
                    remaining_queries = response.headers['X-RateLimit-Remaining']
                    rate_limit_reset = response.headers['X-RateLimit-Reset']
                    check_rate_limits(remaining_queries=remaining_queries, rate_limit_reset=rate_limit_reset, instance=instance_name)

                # Checking if there is more data here
                if 'X-RateLimit-Remaining' in response.headers:
                    remaining_queries = response.headers['X-RateLimit-Remaining']
                    rate_limit_reset = response.headers['X-RateLimit-Reset']
                    check_rate_limits(remaining_queries=remaining_queries, rate_limit_reset=rate_limit_reset, instance=instance_name)

                if 'next' in response.links:
                    condition = True
                    url = response.links['next']['url']
                    iter += 1

                    # Saving a partial list of followers
                    if len(follower_list) % 1000 == 0:
                        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] | Saving checkpoint at iteration = {iter:>5} | total_followers = {len(follower_list):>5}")
                        _data = {'username': username, 'completed': False, 'followers_count': len(follower_list), 'followers': follower_list}
                        export_user_network('followers', username, _data)

                else:
                    condition = False
                    print(f"[{datetime.now().isoformat(timespec='seconds')}] [bold green]Got all followers for {username}![bold green] | total_followers = {len(follower_list):>5}")

            else:
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [italic red]Error {response.status_code} while getting the followers for {username}![/italic red]")
                condition = False

        # We exit the loop either because we reached the maximum number of iterations or because there are no more users to retrieve or because of an error
        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold magenta]Exporting {len(follower_list):>5} follower(s) for {username}![/bold magenta]")
        
        _data = {'username': username, 'completed': True, 'followers_count': len(follower_list), 'followers': follower_list}
        export_user_network('followers', username, _data)

        if store_metadata:
            export_user_network('followers', username+'_metadata', metadata)

    except Exception as e:
        print(f"[{datetime.now().isoformat(timespec='seconds')}][italic red] ({e}) on line {sys.exc_info()[-1].tb_lineno} while getting the follower(s) for {username}![/italic red]")
        return None
    

def get_user_following(username, limit=80, store_metadata=False):
    """
    Method for getting the list of following of a user
    user: user who we want to get the following
    limit: number of following to retrieve for a single call
    store_metadata: whether to store the metadata of the following
    """

    try:
        # Data structures
        following_list = []
        metadata = []
        iter = 1

        # Preparing the headers
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}

        # Getting the account info
        acct, instance_name = username.split('@')
        user_id = None

        # Looking up for the user's id
        lookup_url = f"https://{instance_name}/api/v1/accounts/lookup?acct={acct}"
        lookup_response = requests.get(lookup_url, headers=headers)

        # Getting the user id
        if lookup_response.status_code == 200:
            lookup_page = lookup_response.json()
            if len(lookup_page) == 0:
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] [italic red]User not found![/italic red]")
                return None
            
            user_id = lookup_page['id']
            assert acct == lookup_page['acct'], f"Error: {acct} != {lookup_page['acct']}"
            print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] has ID = {user_id}")

        # We start with a valid condition
        condition = user_id is not None

        # Setting up the url
        url = f"https://{instance_name}/api/v1/accounts/{user_id}/following" 

        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold magenta]Crawling the {username}'s following[/bold magenta]")

        while condition:
            # Setting up the parameters
            params = {'limit': limit}

            # Making the request
            response = requests.get(url, headers=headers, params=params)
            
            # If the request was successful
            if response.status_code == 200:
                data = response.json()

                if len(data) == 0:
                    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] [italic red]No more following to retrieve![/italic red]")

                # If we find local users, we have to add the instance name to the user's username
                for user in data:
                    if '@' not in user['acct']:
                        user['acct'] = user['acct'] + '@' + instance_name

                # We can now get the user list
                following_list.extend([user['acct'] for user in data])
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] | Following = {len(data):>5} | total_following = {len(following_list):>5}")


                if store_metadata:
                    metadata.extend(data)

                # Checking the rate limits
                if 'X-RateLimit-Remaining' in response.headers:
                    remaining_queries = response.headers['X-RateLimit-Remaining']
                    rate_limit_reset = response.headers['X-RateLimit-Reset']
                    check_rate_limits(remaining_queries=remaining_queries, rate_limit_reset=rate_limit_reset, instance=instance_name)

                # Checking if there is more data here
                if 'X-RateLimit-Remaining' in response.headers:
                    remaining_queries = response.headers['X-RateLimit-Remaining']
                    rate_limit_reset = response.headers['X-RateLimit-Reset']
                    check_rate_limits(remaining_queries=remaining_queries, rate_limit_reset=rate_limit_reset, instance=instance_name)

                if 'next' in response.links:
                    condition = True
                    url = response.links['next']['url']
                    iter += 1

                    # Saving a partial list of following
                    if len(following_list) % 1000 == 0:
                        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{username:<19}[/bold] | Saving checkpoint at iteration = {iter:>5} | total_following = {len(following_list):>5}")
                        _data = {'username': username, 'completed': False, 'following_count': len(following_list), 'following': following_list}
                        export_user_network('following', username, _data)
                else:
                    condition = False
                    print(f"[{datetime.now().isoformat(timespec='seconds')}] [bold green]Got all following for {username}![bold green] | total_following = {len(following_list):>5}")

            else:
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [italic red]Error {response.status_code} while getting the following for {username}![/italic red]")
                condition = False

        # We exit the loop either because we reached the maximum number of iterations or because there are no more users to retrieve or because of an error
        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold magenta]Exporting {len(following_list):>5} following for {username}![/bold magenta]")
        
        _data = {'username': username, 'completed': True, 'following_count': len(following_list), 'following': following_list}
        export_user_network('following', username, _data)

        if store_metadata:
            export_user_network('following', username+'_metadata', metadata)

    except Exception as e:
        print(f"[{datetime.now().isoformat(timespec='seconds')}][italic red] ({e}) on line {sys.exc_info()[-1].tb_lineno} while getting the following for {username}![/italic red]")
        return None
    


if __name__ == '__main__':
    # Parameters
    username = 'luciolcw@datasci.social'
    store_metadata = False

    get_user_followers(username, store_metadata=store_metadata)