#!/bin/sh

# it will add same set of tags to the files specified in selection-1

lista=selection-1
while read line
do
		tagi.py add "$line" "$@"
		#tagi.py update "$line" "$@"
done < "$lista"

if [ $? -eq 0 ] 
 then
    echo "remove list"
    rm $lista
fi
