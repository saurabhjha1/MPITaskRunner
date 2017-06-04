#!/bin/bash

if [ "$#" -ne 3 ] || ! [ -d "$3" ]; then
  echo "Usage: $1 FILE $2 grep exp $3 directory" >&2
  exit 1
fi
filename=`echo $1|awk -F'/' '{print $NF}'`
#:echo "Filename is"$filename
/usr/bin/grep -i -n  $2 $1 &> $3/$filename"_grep.raw"
touch $3/$filename".complete" 
