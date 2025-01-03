#!/usr/bin/env bash

PGPORT=${PGPORT:-8812}
CLIENTS=${CLIENTS:-'ALL'}

base_dir=`pwd`

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'psycopg2'* ]]; then
    echo "starting psycopg2 tests"
    python3 -m venv venv/psycopg2_stable
    source venv/psycopg2_stable/bin/activate
    pip install -r compat/src/test/python/requirements_psycopg2_stable.txt
    python compat/src/test/python/runner_psycopg2.py compat/src/test/resources/test_cases.yaml
    if [ $? -ne 0 ]; then
        echo "psycopg2 tests failed"
        exit 1
    fi
    deactivate
    echo "psycopg2 tests finished"
else
    echo "skipping psycopg2 tests"
fi

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'psycopg3'* ]]; then
  echo "starting psycopg3 tests"
  python3 -m venv venv/psycopg3_stable
  source venv/psycopg3_stable/bin/activate
  pip install -r compat/src/test/python/requirements_psycopg3_stable.txt
  python compat/src/test/python/runner_psycopg3.py compat/src/test/resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "psycopg3 tests failed"
      exit 1
  fi
  deactivate
  echo "psycopg3 tests finished"
else
  echo "skipping psycopg3 tests"
fi

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'asyncpg'* ]]; then
  echo "starting asyncpg tests"
  python3 -m venv venv/asyncpg_stable
  source venv/asyncpg_stable/bin/activate
  pip install -r compat/src/test/python/requirements_asyncpg_stable.txt
  python compat/src/test/python/runner_asyncpg.py compat/src/test/resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "asyncpg tests failed"
      exit 1
  fi
  deactivate
  echo "asyncpg tests finished"
else
  echo "skipping asyncpg tests"
fi

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'rust'* ]]; then
  echo "starting rust tests"
  cd compat/src/test/rust/scenarios || exit
  # use well-known versions of dependencies
  cp ./Cargo.unlocked ./Cargo.lock
  cargo build --release
  ./target/release/questrun_rust ../../resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "rust tests failed"
      exit 1
  fi
  echo "rust tests finished"
else
  echo "skipping rust tests"
fi

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'csharp'* ]]; then
  echo "starting csharp tests"

  # check if dotnet is installed
  if ! command -v dotnet &> /dev/null
  then
      echo "dotnet could not be found! Please install .NET 9.0 SDK from https://dotnet.microsoft.com/download/dotnet/9.0"
      exit 1
  fi

  echo "$base_dir/compat/src/test/csharp"
  cd "$base_dir/compat/src/test/csharp" || exit

  # restore dependencies
  dotnet restore

  # build
  dotnet build --configuration Release --no-restore

  # run
  dotnet run --configuration Release --no-build -- ../resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "csharp tests failed"
      exit 1
  fi
  echo "csharp tests finished"
else
  echo "skipping csharp tests"
fi