#!/bin/bash

curl https://api.github.com/repos/questdb/questdb/pulls/$PR_ID/files -s \
      | grep -oP 'filename": "core/src/main/java/io/questdb.*\/\K[^.]+' \
      | tr '\n' ',' | sed -e 's/,/,+:*./g' | sed 's/,+:\*\.$//' \
      | awk '{print "##vso[task.setvariable variable=COVERAGE_DIFF;]+:*." $0}'