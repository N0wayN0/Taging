#!/bin/sh

# this script will update paths of all files in PWD

depth=1
echo "arg0: $0"
echo "arg1: $1"

if [ ! $1 ] 
 then
    dir="$PWD"
else
    dir="$1"
fi

list=$(find "$dir" -maxdepth $depth -type f) # | xargs realpath)
#list=echo "$list" | tr "\n" " "
echo "$list" 

tagi.py update_path "$list"

