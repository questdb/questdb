#!/usr/bin/env bash
echo "##vso[task.setvariable variable=COVERAGE_DIFF;]$(

curl https://api.github.com/repos/questdb/questdb/pulls/977/files -s \
      | grep -oP 'filename": "core/src/main/java/io/questdb.*\/\K[^.]+' \
      | tr '\n' ',' | sed -e 's/,/,+:*./g' \
      | awk '{print "##vso[task.setvariable variable=COVERAGE_DIFF;]+:*." $0}'