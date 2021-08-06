#!/bin/bash

COV_CLASSES=$(curl https://api.github.com/repos/questdb/questdb/pulls/$1/files?per_page=100 -s \
      | grep -oP 'filename": "core/src/main/java/io/questdb.*\/\K[^.]+' \
      | tr '\n' ',' | sed -e 's/,/,+:*./g' | sed 's/,+:\*\.$//')

if [ -z "$COV_CLASSES" ]
then
    echo "##vso[task.setvariable variable=COVERAGE_DIFF;]-:*"
    echo "##vso[task.setvariable variable=CODE_COVERAGE_TOOL_OPTION;]None"
else
    ALL_CHANGES=$COV_CLASSES
    PAGE=1
    while true; do
      PAGE=$((PAGE+1))
      COV_CLASSES=$(curl https://api.github.com/repos/questdb/questdb/pulls/$1/files?per_page=100\&page=$PAGE -s \
        | grep -oP 'filename": "core/src/main/java/io/questdb.*\/\K[^.]+' \
        | tr '\n' ',' | sed -e 's/,/,+:*./g' | sed 's/,+:\*\.$//')
      if [ -z "$COV_CLASSES" ]
      then
        break
      else
        ALL_CHANGES="${ALL_CHANGES},+:*.${COV_CLASSES}"
      fi
    done

    echo "##vso[task.setvariable variable=COVERAGE_DIFF;]+:*.$ALL_CHANGES"
    echo "##vso[task.setvariable variable=CODE_COVERAGE_TOOL_OPTION;]JaCoCo"

    IS_FORK=$(curl https://api.github.com/repos/questdb/questdb/pulls/$1 -s | grep -e "\"fork\": true")
    if [ -z "$IS_FORK" ]
    then
      echo "##vso[task.setvariable variable=IF_FORK;]NO"
    else
      echo "##vso[task.setvariable variable=IF_FORK;]YES"
    fi
fi

