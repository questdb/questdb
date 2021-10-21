#!/bin/bash

################################################################################
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2020 QuestDB
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
################################################################################

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

