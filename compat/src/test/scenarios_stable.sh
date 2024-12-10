#!/usr/bin/env bash

PGPORT=${PGPORT:-8812}
CLIENTS=${CLIENTS:-'ALL'}

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
  cd compat/src/test/rust/scenarios
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