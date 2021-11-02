#!/bin/bash

git status --porcelain | awk '{print $2}' | xargs ls -hs | sort -h
