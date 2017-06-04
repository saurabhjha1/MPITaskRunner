#!/bin/bash
if [ "$#" -ne 2 ] || ! [ -d "$2" ]; then
  echo "Usage: $1 FILE $2 DIRECTORY" >&2
  exit 1
fi

filename=`echo $1|awk -F'/' '{print $NF}'`
#:echo "Filename is"$filename
tar -zcf $2/$filename".tar.gz" $1
touch $2/$filename".complete" 
