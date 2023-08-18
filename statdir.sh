#!/bin/sh

# this script show all taged and not taged files in given directory

if [ !$1 ] 
 then
    dir="$PWD"
else
    dir="$1"
fi

tagi.py stat_dir "$dir"


