#!/usr/bin/env python
import argparse
import random
import subprocess as sp
import time
import json
import requests


def get_cache_list(args):
    with open(args.cache_members, 'r') as fd:
        t_members = json.load(fd)['CacheList']
        return t_members[0:int(args.n_caches)]



def restart_dask_componenets(args):
    print('Stop all dask workers .......')
    sp.run('''ps auxwww | grep dask.worker | grep -v grep | awk '{print "kill -9 " $2}' | sh''', shell=True, check=True)

    print('Kill all dask scheduler .....')
    sp.run('''ps auxwww | grep dask.scheduler | grep -v grep | awk '{print "kill -9 " $2}' | sh''', shell = True, check = True)

    print('Start dask scheduler .....')
    sp.Popen(['python3', f'{args.dask_dir}/distributed/cli/dask_scheduler.py', '--coloring-verbose-dir', f'{args.dask_verbose_dir}'])
    time.sleep(2) # just wait for dask logs 

    base_worker_port = random.randint(10000, 50000)
    for i in range(0, int(args.ndelegate)):
        print(f'Start delegate workers {i} .....')
        start_delegate_worker(base_worker_port = base_worker_port + i, args=args)
        time.sleep(2) # just wait for 2 seconds




def start_delegate_worker(args, base_worker_port=10000):
    scheduler = {'color_round_robin': 'consistent_round_robin',
            'consistent_hashing': 'consistent_hashing',
            'random': 'random',
            'max_parallelism': 'random',
            'round_robin': 'rr'}

    storage_backend = 'azurecache' if args.serialization == 'persist' else 'azurecache-ns'

    serialization = '_a' if args.serialization == 'arrow' else '_d' if args.serialization == 'dill' else ''
    cache_sync = 's' if args.cache_sync == 'sync' else 'a'
    backend_sync = 's' if args.backend_sync == 'sync' else 'a'    
    run_task_prefix = f'api/run_task{serialization}_cp{cache_sync}{backend_sync}'
    get_task_prefix = f'api/get_data{serialization}_cps'
    
    kargs = ['python3' , f'{args.dask_dir}/distributed/cli/dask_worker.py', 
            'localhost:8786',
            '--worker-class', 'distributed.worker.DelegatingWorker',
            '--no-nanny', '--nprocs', '1',
            '--worker-port', f'{base_worker_port}',
            '--storage_backend', f'{storage_backend}',
            '--functions_lb', f'{scheduler[args.scheduler]}',
            '--data_locality', f'{args.data_locality}',
            '--run_task_url', f'{run_task_prefix}',
            '--get_data_url', f'{get_task_prefix}',
            '--num_shotgun', f'{args.shotgun_factor}']

    cache_list = get_cache_list(args)
    for ci in cache_list:
        kargs.append('--storage_arg')
        kargs.append(f'{ci["address"]},{ci["name"]}')

    proc = sp.Popen(kargs)
    return proc
    

def list_cache_membership(args):
    cache_list = get_cache_list(args)
    available_caches = 0
    for cache in cache_list:
        print("Membership List for {}:".format(cache['address']))
        url = "{}/debug_info".format(cache['address'])
        r = requests.get(url)
        if (r.status_code == 200):
            try:
                print("   ","\n    ".join(r.json()["Membership"]["MembershipList"]))
                print("   Size: {}/{} ({}); Evicted: {}".format(r.json()["LocalCache"]["TotalSizeString"],
                                                                r.json()["LocalCache"]["MaxSizeString"],
                                                                r.json()["LocalCache"]["Utilization"],
                                                                r.json()["LocalCache"]["NumObjectsEvicted"]))
                print("   Number of Python Workers: {} / {}".format(r.json()["NumPythonProcesses"],
                                                               r.json()["SystemInfo"]["EnvironmentVariables"]["FUNCTIONS_WORKER_PROCESS_COUNT"]))
                available_caches += (int(r.json()["NumPythonProcesses"]) == int(r.json()["SystemInfo"]["EnvironmentVariables"]["FUNCTIONS_WORKER_PROCESS_COUNT"]))
            except Exception as e:
                print("Can't get {} membership: {}".format(cache['address'], e))
        else:
            print("Can't get {} membership: {}".format(cache['address'], r.status_code))
    return available_caches == args.n_caches



def set_cache_membership(args):
    resize = args.cache_size
    cache_list = get_cache_list(args)
    for cache in cache_list:
        print("Membership List for {}:".format(cache['address']))
        for peer in cache_list:
            url = "{}/membership?op=add&arg={}".format(cache['address'], peer['name'])
            #print(url)
            try:
                r = requests.get(url)
                print(" cache {} -> ({}): {}".format(cache['address'], r.status_code, r.text))
            except Exception as e:
                print(e)
        if resize:
            url = "{}/cache?op=maxsize&arg={}".format(cache['address'], resize)
            try:
                r = requests.get(url)
                print(" cache {} -> ({}): {}".format(cache['address'], r.status_code, r.text))
            except Exception as e:
                print(e)
        url = "{}/debug_info".format(cache['address'])
        r = requests.get(url)
        if (r.status_code == 200):
            try:
                print("   ","\n    ".join(r.json()["Membership"]["MembershipList"]))
            except Exception as e:
                print("Can't get {} membership: {}".format(cache['address'], e))
            if (resize):
                try:
                    print("    MaxSize: " + (r.json()["LocalCache"]["MaxSizeString"]))
                except Exception as e:
                    print("Can't get {} maxsize: {}".format(cache['address'], e))
        else:
            print("Can't get {} membership: {}".format(cache['address'], r.status_code))




def restart_caches(args):
    cache_list = get_cache_list(args)
    for cache in cache_list:
        url = "{}/terminate".format(cache['address'])
        print("Resetting {}...".format(url))
        try:
            r = requests.get(url, timeout=0.01)
        except requests.exceptions.Timeout as e:
            pass  #not expecting a response
        except Exception as e:
            print(e)

    print('Sleep for 30 seconds after restarting the caches')
    time.sleep(30)

    while not list_cache_membership(args): continue
    set_cache_membership(args)
    while not list_cache_membership(args): continue
    return


def dump_cache_list(args):
    caches = {'CacheList' : get_cache_list(args)}
    with open('cache-list.json', 'w') as fd:
        json.dump(caches, fd, indent=3)



def warmup_cache(args):
    print("Warm up cache")
    for i in range(0, 3):
        sp.run(['python3', './test_matrix.py', '--scheduler', 'localhost:8786', '--test', 'shuffle', '--par', '24', '--mem', '10'])
