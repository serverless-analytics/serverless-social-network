#!/usr/bin/env python
import sys
import json

in_path = sys.argv[1]
out_path = sys.argv[2]

with open(in_path, 'r') as rfd, open(out_path, 'w') as wfd:
    traces = json.load(rfd)
    wfd.write("oid,size,type,author\n")
    for tr in traces:
        objects = list(tr.values())[0]['objects']
        for obj in objects:
            wfd.write(f'{obj["oid"]},{obj["size"]},{obj["type"]},{obj["author"]}\n')

