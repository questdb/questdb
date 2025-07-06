#!/usr/bin/env bash

HELLO_FILE=$1
for a in {1..80}; do
 if [ -f $HELLO_FILE ]; then
   cat $HELLO_FILE
   rm $HELLO_FILE
   break
 fi
 sleep 0.25
done
