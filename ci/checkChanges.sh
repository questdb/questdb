#!/bin/bash

if [ -z "$1" ]
then
  # No PR - cannot detect changed files, default to true
  echo "##vso[task.setvariable variable=SOURCE_CODE_CHANGED;isOutput=true]true"
else
  CHANGED_CORE_FILES=$(curl https://api.github.com/repos/questdb/questdb/pulls/$1/files -s  | grep -oP 'filename": "core/*\/\K[^.]+')

  if [ -z "$CHANGED_CORE_FILES" ]
  then
      echo "##vso[task.setvariable variable=SOURCE_CODE_CHANGED;isOutput=true]false"
  else
      echo "##vso[task.setvariable variable=SOURCE_CODE_CHANGED;isOutput=true]true"
  fi
fi