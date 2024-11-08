#!/usr/bin/env bash

export PGPORT=8812

echo "starting psycopg2 tests"
python3 -m venv venv/psycopg2_latest
source venv/psycopg2_latest/bin/activate
pip install -r compat/src/test/python/requirements_psycopg2_latest.txt
python compat/src/test/python/runner_psycopg2.py compat/src/test/resources/test_cases.yaml
if [ $? -ne 0 ]; then
    echo "psycopg2 tests failed"
    exit 1
fi
deactivate
echo "psycopg2 tests finished"

echo "starting psycopg3 tests"
python3 -m venv venv/psycopg3_latest
source venv/psycopg3_latest/bin/activate
pip install -r compat/src/test/python/requirements_psycopg3_latest.txt
python compat/src/test/python/runner_psycopg3.py compat/src/test/resources/test_cases.yaml
if [ $? -ne 0 ]; then
    echo "psycopg3 tests failed"
    exit 1
fi
deactivate
echo "psycopg3 tests finished"

echo "starting asyncpg tests"
python3 -m venv venv/asyncpg_latest
source venv/asyncpg_latest/bin/activate
pip install -r compat/src/test/python/requirements_asyncpg_latest.txt
python compat/src/test/python/runner_asyncpg.py compat/src/test/resources/test_cases.yaml
if [ $? -ne 0 ]; then
    echo "asyncpg tests failed"
    exit 1
fi
deactivate
echo "asyncpg tests finished"

echo "starting rust tests"
cd compat/src/test/rust/scenarios
rm -f ./Cargo.lock
cargo build --release
./target/release/questrun_rust ../../resources/test_cases.yaml
if [ $? -ne 0 ]; then
    echo "rust tests failed"
    exit 1
fi
echo "rust tests finished"