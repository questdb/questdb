#!/usr/bin/env bash

HELLO_FILE=$1
for a in {1..80}; do
 if [ -f "$HELLO_FILE" ]; then
   cat "$HELLO_FILE" 2>/dev/null
   rm "$HELLO_FILE" 2>/dev/null
   break
 fi
 sleep 0.25
done
