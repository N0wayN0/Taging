#!/bin/bash

# update paths to all files in all subdirectories. There will be a lot of files more than 1000 maybe, more than shell
# can handle. So it will ubpade by batches of $to_display

filename="/tmp/lista.txt"
to_display=1000
start_line=1

find "$PWD" -type f > "$filename"
all_files=$(wc -l < "$filename")
#lista=$(find . -type f | shuf | head -n 1248)

while true; do
    end_line=$(($start_line + $to_display -1))
    echo to_display $to_display
    echo start_line $start_line
    echo end_line $end_line
    echo all_files $all_files
    #read -p "Press Enter ..."
    
    lista=$(sed -n "$start_line,$end_line p" "$filename")
    tagi.py update_path "$lista"
    
    #read -p "Press Enter ..."

    start_line=$((end_line + 1))

    if [ $start_line -gt "$all_files" ]; then
        break
    fi
done
