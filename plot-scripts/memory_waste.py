#!/usr/bin/python

from threading import Thread 
from threading import get_ident
import random
import math

import pandas as pd
import numpy as np
import seaborn as sns
import os
import json
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.ticker as ticker



def build_mem_df(fpath):
    global objects
    with open(fpath, 'r') as fd:
        data = sorted(json.load(fd), key=lambda k: k['timestamps']['main_end_ms'])
        obj_in_cache = {}
        mem_usage = []
        for index, rd in enumerate(data):
            req_id = rd['request_id']
            req_ts = rd['timestamps']['main_start_ms']
            post_status = rd['object_access']

            for post in post_status:
                for obj_s in post:
                    oid = obj_s['oid']
                    if oid not in obj_in_cache:
                        obj_in_cache[oid] = {'caches': set()}
                    obj_in_cache[oid]['caches'].add(obj_s['name'])

                    for e_oid in obj_s['evicted']:
                        if e_oid not in obj_in_cache:
                            print(f'why was {e_oid} not found?')
                            continue
                        obj_in_cache[e_oid]['caches'].discard(obj_s['name'])

            if index%100 == 99:
                post_mem = 0
                for p_oid in obj_in_cache:
                    if p_oid not in objects:
                        print(f'This is very odd! {p_oid}  I think this happend becauseo of lost requests')
                    obj_size = objects[p_oid] if p_oid in objects else 1
                    if len(obj_in_cache[oid]['caches']) == 0:
                        obj_size = 0

                    post_mem += (len(obj_in_cache[oid]['caches']) -1)*obj_size
                mem_usage.append({'timestamp': req_ts,
                                 'mem_sz': post_mem})

            if index%100 == 99:
                print(f'{get_ident()} {index} out of {len(data)}')
            #if index%1000==999:
            #    break

        df_mem = pd.DataFrame(mem_usage)
        df_mem['relative'] = df_mem['timestamp'] - df_mem['timestamp'].min()
    return df_mem

def plot_memusage_per_cache(df):
    df['ts_min'] = df['relative']//10000
    df_mem = df.groupby('ts_min').agg({'mem_sz': 'max'}).reset_index()
    sns.set_style("ticks")

    sns.set_context("paper", font_scale=1)
    sns.set_context(rc = {'patch.linewidth': 1.5, 'patch.color': 'black'})

    plt.rc('font', family='serif')

    fig, ax = plt.subplots(figsize=(8,4))
    sns.scatterplot(x='ts_min', y='mem_sz', data=df_mem, ax=ax)
    sns.despine()

    ax.yaxis.grid(color='#99999910', linestyle=(0, (5, 10)), linewidth=0.4)
    ax.set_axisbelow(True)

    # plotting
    ax.set_xlabel('# of func executors (= # of parallel invocations)', fontsize=14)
    ax.set_ylabel('hit ratio', fontsize=14)
    ax.set_title('Object size distribution', fontsize=14)

    ax.tick_params(axis='x', which='major', labelsize=14, rotation=15)
    ax.tick_params(axis='y', which='major', labelsize=14)
    ax.yaxis.set_ticks_position('both')
    plt.show()
    return


def run(fpath):
    df = build_mem_df(fpath)
    df.to_csv(fpath.rsplit('/', 1)[1].replace('.json', '.csv'),index=False)
    #plot_memusage_per_cache(df)



objects = {}


crr_fpath = os.path.join(f'/local0/serverless-social-network/results/zipf0.9/trace.zipf-0p9.60min.72p.24.crr.json')
with open(crr_fpath, 'r') as fd:
    ref_data = json.load(fd)
    ref_caches = []
    objects_access = []
    objects_meta = []
    for rd in ref_data:
        req_id = rd['request_id']
        req_ts = rd['timestamps']['main_start_ms']
        objs_t = rd['objects']
        for obj in objs_t:
            if obj['oid'] not in objects:
                objects[obj['oid']] = obj['size']
    

#print(json.dumps(objects, indent=4))



import json
import pandas
from os import listdir
from os.path import isfile, join
import pandas as pd 

wdir = '/local0/serverless-social-network/results/zipf0.9'
logfiles = [f for f in listdir(wdir) if isfile(join(wdir, f))]

trace_collection = {}
for fpath in logfiles:
    trace, n_workers, policy, _ = fpath.rsplit('.', 3)
    if n_workers not in trace_collection:
        trace_collection[n_workers] = {}
    if trace not in trace_collection[n_workers]:
        trace_collection[n_workers][trace] = []
    trace_collection[n_workers][trace].append(fpath)
    
print('Traces that are going to be parsed', json.dumps(trace_collection, indent=4))



threads = []
for n_workers in trace_collection:
    for tname in trace_collection[n_workers]:
        for exec_name in trace_collection[n_workers][tname]:
            fpath = os.path.join(wdir, exec_name)
            t = Thread(target = run, args=(fpath,))
            threads.append(t)
            t.start()

for t in threads:
    t.join()
    #break
