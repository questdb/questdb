#!/usr/bin/env bash

PGPORT=${PGPORT:-8812}
base_dir=`pwd`

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

echo "starting rust-tokio tests"
cd compat/src/test/rust-tokio
rm -f ./Cargo.lock
cargo build --release
./target/release/questrun_rust ../resources/test_cases.yaml
if [ $? -ne 0 ]; then
    echo "rust-tokio tests failed"
    exit 1
fi
cd "$base_dir" || exit
echo "rust-tokio tests finished"

echo "starting rust-sqlx tests"
cd compat/src/test/rust-sqlx
rm -f ./Cargo.lock
cargo build --release
./target/release/questrun_sqlx ../resources/test_cases.yaml
if [ $? -ne 0 ]; then
    echo "rust-sqlx tests failed"
    exit 1
fi
cd "$base_dir" || exit
echo "rust-sqlx tests finished"

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