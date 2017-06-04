#!/bin/bash

if [ "$#" -ne 2 ] || ! [ -d "$2" ]; then
  echo "Usage: $1 FILE $2 DIRECTORY" >&2
  exit 1
fi
filename=`echo $1|awk -F'/' '{print $NF}'`
#:echo "Filename is"$filename
/usr/bin/split $1 --lines=23741058 -da 3 $2/$filename"_split.raw"
touch $2/$filename".complete" 
