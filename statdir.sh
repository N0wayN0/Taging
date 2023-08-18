#!/bin/sh

if [ !$1 ] 
 then
    dir="$PWD"
else
    dir="$1"
fi

tagi.py stat_dir "$dir"


