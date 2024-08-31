# Libraries
import json
import os
import sys
from datetime import datetime
from time import time, sleep

import requests
from rich import print

from utils import *

import threading

lock = threading.Lock()

def init_instance(instance_name):
    # If the instance was not resumed nor processed, we start from scratch
    # Creating the processed.json if it does not exist to store metadata
    with lock:
        if not os.path.exists('__mastodonte__'):
            # Creating the folder if it does not exist
            os.makedirs('__mastodonte__', exist_ok=True)

            # Creating the initial json
            with open('__mastodonte__/timelines_processed.json', 'w') as _f:
                diz = {}
                diz[instance_name] = {'last_seen': 1, 'processed': False, 'overall_data': 0}
                json.dump(diz, _f, indent=4)
                
        else:
            # If the instance is not in the processed.json, we add it
            with open('__mastodonte__/timelines_processed.json', 'r') as _f:
                processed_instances = json.load(_f)
                if instance_name not in processed_instances:
                    processed_instances[instance_name] = {'last_seen': -1, 'processed': False, 'overall_data': 0}
            with open('__mastodonte__/timelines_processed.json', 'w') as _f:
                json.dump(processed_instances, _f, indent=4)  



def update_metadata(instance_name, last_seen, overall_data, processed):
    with lock:
        with open('__mastodonte__/timelines_processed.json', 'r') as _f:
            processed_instances = json.load(_f)
            
            if instance_name not in processed_instances:
                raise Exception(f"Instance {instance_name} not found in the processed.json!")
            
            processed_instances[instance_name]['last_seen'] = last_seen
            processed_instances[instance_name]['overall_data'] = overall_data
            processed_instances[instance_name]['processed'] = processed

        with open('__mastodonte__/timelines_processed.json', 'w') as _f:
            json.dump(processed_instances, _f, indent=4)



def get_timeline(instance_name, max_iter, only_local=True, get_reblogging_users=False, resume=True, tolerance=5):
    """
    Main method for the extraction of the timelines
    url: link to the instance
    max_iter: max number of iterations to perform (each iter will collect up to 40 posts)
    only_local: option to collect only local posts
    get_reblogging_users: option to collect reblogging users
    resume: option to resume from the last seen post
    """

    try:
        last_seen = -1
        overall_data = 0
        statuses = []

        # If resume is enabled, checking the processed.json
        if resume:
            previous_records = previous_data(operation='timelines', instance_name=instance_name)
            if previous_records is not None:
                last_seen = previous_records.get('last_seen', -1)
                completed = previous_records.get('processed', None)
                overall_data = previous_records.get('overall_data', 0)
                if completed:
                    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold cyan]{instance_name}[/bold cyan] was already processed! (last_seen_id = {last_seen})")
                    return
                elif last_seen != -1:
                    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold cyan]{instance_name}[/bold cyan] Resuming from last_seen_id = {last_seen} | previous_data = {overall_data}")
            else:
                print(f"[{datetime.now().isoformat(timespec='seconds')}] [bold cyan]{instance_name}[/bold cyan] was not found in the processed.json, starting from scratch!")
                
        # Initializing the instance if it was not resumed nor processed
        if last_seen == -1:
            init_instance(instance_name)

        # Start operating (iterate while no data remains)
        condition = True
        while condition:
            try:
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold magenta]Crawling {instance_name}[/bold magenta] (last_seen_id = {last_seen})")

                # URLs and headers
                header_network = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}
                params = {'limit': '40'}
                if only_local:
                    params['local'] = 'true'
                else:
                    params['local'] = 'false'
                
                print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold cyan]Current configuration for {instance_name}:[/bold cyan] {params}")
                
                # Resume if already explored partially
                if last_seen != -1:
                    params['max_id'] = last_seen

                # Preparing the url    
                url_timeline = f"https://{instance_name}/api/v1/timelines/public"

                # Some info
                current_iter = 0
                total_statuses = 0
                reblog_count = 0 if get_reblogging_users else 'NA'
                reblogged_statuses = 0 if get_reblogging_users else 'NA'

                while current_iter < max_iter and condition:
                    sleep(1)  # Being polite is always fun! :)

                    # TBD: preliminary lookup for search limits here

                    # Let's read the timeline!
                    page_response = requests.get(url_timeline, 
                                                 headers=header_network, 
                                                 params=params, 
                                                 timeout=30)

                    # Reset last_seen, no more needed
                    if 'max_id' in params:
                        del params['max_id']

                    # If the response is ok
                    if page_response.status_code == requests.codes.ok:
                        page_response_content = page_response.json()

                        if len(page_response_content) > 0:

                            for status in page_response_content:
                                last_seen = status['id']

                                # Checking if we need rebloggers too
                                if get_reblogging_users:  
                                    # Checking if it has re-blogs too
                                    if status['reblogs_count'] > 0:
                                        # Preparing reblogs url and params
                                        url_reblogs = f"https://{instance_name}/api/v1/statuses/{last_seen}/reblogged_by"
                                        params_reblogs = {'limit': '80'}

                                        # Getting reblogging users
                                        rebloggers = get_reblogs(url=url_reblogs, 
                                                                header=header_network, 
                                                                params=params_reblogs, 
                                                                instance=instance_name)

                                        # We need to get reblogs!
                                        reblog_count += len(rebloggers)
                                        reblogged_statuses += 1

                                        # Updating the status info
                                        status['rebloggers'] = rebloggers
                                
                                # Appending the status to the list
                                statuses.append(status)
                                total_statuses += 1
                                overall_data += 1

                            # Ok, I've just processed the batch of statuses
                            print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')}] [bold]{instance_name:<19}[/bold] | Statuse(s) = {total_statuses:>5} | Reblogged = {reblogged_statuses:>5} | Reblog(s) = {reblog_count:>5} | last_TL_ID = {last_seen:<18} | overall_data = {overall_data:>5}")

                            # Updating last_seen
                            try:                        
                                update_metadata(instance_name=instance_name, last_seen=last_seen, overall_data=overall_data, processed=False)
                            except Exception as e:
                                print("Exception:", e)

                            # Saving each 100 statuses
                            if len(statuses) > 100:
                                export_data(operation='timelines', instance_name=instance_name, data=statuses)
                                statuses = []

                            # Checking if there is more data here
                            if 'X-RateLimit-Remaining' in page_response.headers:
                                remaining_queries = page_response.headers['X-RateLimit-Remaining']
                                rate_limit_reset = page_response.headers['X-RateLimit-Reset']
                                check_rate_limits(remaining_queries=remaining_queries, rate_limit_reset=rate_limit_reset, instance=instance_name, tolerance=tolerance)

                            if 'next' in page_response.links:
                                condition = True
                                current_iter += 1
                                url_timeline = page_response.links['next']['url']
                            else:
                                condition = False

                                # If there is more data waiting for being exported, do it!
                                if len(statuses) > 0:
                                    export_data(instance_name, statuses)
                                    statuses = []

                                # We completed the timeline of the instance, we can mark it as processed
                                # Updating last_seen
                                update_metadata(instance_name=instance_name, last_seen=last_seen, overall_data=overall_data, processed=True)

                                print(f"[{datetime.now().isoformat(timespec='seconds')}] [bold green]Timeline completed for {instance_name}![/bold green] | last_TL_ID = {last_seen:<25} | overall_data = {overall_data:>5}")

                        # No content
                        else:
                            condition = False
                            print(f"[{datetime.now().isoformat(timespec='seconds')}] [bold red]No content here for {instance_name}, stopping![/bold red] | last_TL_ID = {last_seen:<25}")


                # Status code is not ok?
                else:
                    condition = False
                    print(f"[{datetime.now().isoformat(timespec='seconds')}] [bold red]max_iter reached for {instance_name}, stopping![/bold red] | last_TL_ID = {last_seen:<25} | overall_data = {overall_data:>5}")

            except Exception as e:
                print(f"[{datetime.now().isoformat(timespec='seconds')}][italic red] ({e}) on line {sys.exc_info()[-1].tb_lineno} while crawling the timeline![/italic red]")
                
                # Updating last_seen
                update_metadata(instance_name=instance_name, last_seen=last_seen, overall_data=overall_data, processed=False)
                
    
    except Exception as e:
        print(f"[{datetime.now().isoformat(timespec='seconds')}][italic red] ({e}) on line {sys.exc_info()[-1].tb_lineno} while initializing the crawling for {instance_name}![/italic red]")


def get_reblogs(url, header, params, instance):
    """
    Paginate over all available pages and extract rebloggers
    url: link indicating reblogged_by page
    header: header to be used for the request
    params: any params involving the request (e.g., max number of items to return for each request)
    instance: server where the request should be sent
    """
    # Explored users
    users = []

    # First request is expected
    condition = True

    while condition:
        sleep(1) # Being polite is always fun! :)
        page_response = requests.get(url, headers=header, params=params, timeout=10)
        if page_response.status_code == requests.codes.ok:
            page_response_content = page_response.json()

            if len(page_response_content) > 0:
                for user in page_response_content:
                    # If it's local, fix the handle
                    user_acct = user['acct']
                    if '@' not in user_acct:
                        user_acct = user_acct + '@' + instance
                   
                    # Adding to the list
                    users.append(user_acct.lower())

                if 'X-RateLimit-Remaining' in page_response.headers:
                    remaining_queries = page_response.headers['X-RateLimit-Remaining']
                    rate_limit_reset = page_response.headers['X-RateLimit-Reset']
                    check_rate_limits(remaining_queries, rate_limit_reset, instance)

                if 'next' in page_response.links:
                    condition = True
                    url = page_response.links['next']['url']

                else:
                    condition = False
            else:
                condition = False

        # If not ok, we should stop iterating
        else:
            condition = False

    return users



def get_partitions(instances_list, n: int):
    """
    Partition instances to be processed according to the number of available threads
    instances_list: the list of instances to be processed
    n : number of available threads to be used in the crawling
    """
    # Let's create N partition (N crawlers)
    partitions = [[] for _ in range(n)]
    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')} | get_partitions][italic green] {len(partitions)} empty partition(s) created[/italic green]")
    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')} | get_partitions][italic green] {len(instances_list)} instance(s) to be explored..[/italic green]")

    # Let's split data into chunks, each chunk will be processed by a thread
    for index, instance in enumerate(instances_list):
        partitions[index % n].append(instance)

    # Partitioning completed
    counter = 0
    for index, partition in enumerate(partitions):
        counter += len(partition)
        print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')} | get_partitions][italic] Partition {index + 1} contains {len(partition)} element(s)[/italic]")

    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')} | get_partitions][italic] Original items to be distributed = {len(instances_list)}[/italic]")
    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')} | get_partitions][italic] Distributed items = {counter}[/italic]")
    print('-' * 30)

    return partitions



def handle_multiple_instances(partition, max_iter, only_local, get_reblogging_users, resume, num_threads):
    """
    During the parallel crawling, each thread will handle multiple instances in a list
    instances_list: list of instances to be processed
    """
    for instance in partition:
        # Getting the timeline
        get_timeline(instance_name=instance, max_iter=max_iter, only_local=only_local, get_reblogging_users=get_reblogging_users, resume=resume, tolerance=num_threads)



def parallel_crawling(list_path, num_threads, max_iter, only_local=True, get_reblogging_users=False, resume_when_possible=True):
    """
    Get the timelines for multiple instances in parallel
    list_path: path to the list of instances
    num_threads: number of threads to be used
    """

    # Reading the list of instances
    with open(list_path, 'r') as _f:
        instances = _f.readlines()

    # Preparing the list of instances
    instances = [x.strip() for x in instances]

    # Splitting the instances into chunks w.r.t. the number of threads
    partitions = get_partitions(instances, num_threads)

    # Preparing the threads
    threads = []

    for partition in partitions:
        threads.append(threading.Thread(target=handle_multiple_instances, args=(partition, max_iter, only_local, get_reblogging_users, resume_when_possible, num_threads)))

    start_time = time()
    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')} | parallel_crawling][bold green] Starting the threads![/bold green]")

    # Starting the threads
    for thread in threads:
        thread.start()

    # Waiting for the threads to finish
    for thread in threads:
        thread.join()

    print(f"[{datetime.now().isoformat(timespec='seconds', sep=' ')} | start_crawling][bold blue] Time elapsed = {time() - start_time} sec.[/bold blue]")


if __name__ == '__main__':
    # instance_name = input("Enter the instance name: ")
    # max_iter = int(input("Enter the max number of iterations (x40 toots): "))
    # only_local = input("Only local toots? (y/n): ")
    # get_reblogging_users = input("Get reblogging users? (y/n): ")
    # resume = input("Resume from the last seen ID? (y/n): ")
    # get_timeline(instance_name, max_iter, only_local=='y', get_reblogging_users=='y', resume=='y')

    # Testing the parallel crawling
    parallel_crawling('instances.txt', 3, 100, True, False, True)
    