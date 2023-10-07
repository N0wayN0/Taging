#!/bin/sh

# this script show all taged and not taged files in given directory

echo "arg0: $0"
echo "arg1: $1"

if [ ! $1 ] 
 then
    dir="$PWD"
else
    dir="$1"
fi

tagi.py stat_dir "$dir"

