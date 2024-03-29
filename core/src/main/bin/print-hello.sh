#!/usr/bin/env bash
hello_file=$1/log/hello.txt

for a in {1..80}; do
 if [ -f $hello_file ]; then
   cat $hello_file
   rm $hello_file
   break
 fi
 sleep 0.25
done
