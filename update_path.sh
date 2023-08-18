#!/bin/sh


lista=$(find "$PWD" -maxdepth 1 -type f | xargs realpath)
# script sprawdzi wszystkie pliki na liscie za jedym razem
tagi.py update_path $lista

exit

for line in $lista
do
    echo "$line"
done


lista=selection-1
while read line
do
		tagi.py update_path "$line"
done < "$lista"

if [ $? -eq 0 ] 
 then
    echo "remove list"
    rm $lista
fi
