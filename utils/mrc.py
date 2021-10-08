#!/usr/bin/env python
import sys
import json
from cache import LruCache
import pandas as pd

trace_fpath = sys.argv[1]

df = pd.read_csv(trace_fpath)

print(df)



'''
cache_size = 5*100*1024*1024
cache = LruCache(cache_size)
hit = 0
miss = 0



with open(trace_fpath, 'r') as fd:
    ln = fd.readline().split(',')
    while True:
        ln = fd.readline()
        if not ln:
            break
        oid, size, _type, user = ln.replace('\n', '').split(',')
        size = int(size)
        value = cache.get(oid)
        if value == -1: 
            cache.put(oid, value=size, size=size)
            miss += 1
        else:
            hit += 1
    print('# of hits: ', hit, '# of miss', miss, 'hit ratio', hit/(hit + miss))
'''
