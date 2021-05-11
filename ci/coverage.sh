#!/bin/bash

COV_CLASSES=$(curl https://api.github.com/repos/questdb/questdb/pulls/$1/files -s \
      | grep -oP 'filename": "core/src/main/java/io/questdb.*\/\K[^.]+' \
      | tr '\n' ',' | sed -e 's/,/,+:*./g' | sed 's/,+:\*\.$//')

if [ -z "$COV_CLASSES" ]
then
    echo "##vso[task.setvariable variable=COVERAGE_DIFF;]-:*"
    echo "##vso[task.setvariable variable=CODE_COVERAGE_TOOL_OPTION;]None"
else
    echo "##vso[task.setvariable variable=COVERAGE_DIFF;]+:*.$COV_CLASSES"
    echo "##vso[task.setvariable variable=CODE_COVERAGE_TOOL_OPTION;]JaCoCo"
fi