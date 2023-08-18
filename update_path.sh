#!/bin/sh

# this script will update paths of all files in PWD

list=$(find "$PWD" -maxdepth 1 -type f | xargs realpath)
tagi.py update_path $list

