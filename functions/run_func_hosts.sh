#!/bin/bash

n_hosts=5
base=37000

for i in $(seq 1 1 ${n_hosts})
do
   port=$((${base} + ${i}))
   echo "Run function host port: ${port}"
   func host start --port $port &
done

