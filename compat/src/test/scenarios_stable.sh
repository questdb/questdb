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
  echo "starting rust-tokio tests"
  cd compat/src/test/rust-tokio || exit
  # use well-known versions of dependencies
  cp ./Cargo.unlocked ./Cargo.lock
  cargo build --release
  ./target/release/questrun_rust ../resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "rust-tokio tests failed"
      exit 1
  fi
  cd "$base_dir" || exit
  echo "rust-tokio tests finished"
else
  echo "skipping rust-tokio tests"
fi

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'sqlx'* ]]; then
  echo "starting rust-sqlx tests"
  cd compat/src/test/rust-sqlx || exit
  # use well-known versions of dependencies
  cp ./Cargo.unlocked ./Cargo.lock
  cargo build --release
  ./target/release/questrun_sqlx ../resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "rust-sqlx tests failed"
      exit 1
  fi
  cd "$base_dir" || exit
  echo "rust-sqlx tests finished"
else
  echo "skipping rust-sqlx tests"
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

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'php'* ]]; then
  echo "starting php tests"

  # check if php is installed
  if ! command -v php &> /dev/null
  then
      echo "php could not be found! Please install PHP or exclude PHP tests"
      exit 1
  fi

  echo "$base_dir/compat/src/test/php"
  cd "$base_dir/compat/src/test/php" || exit

  # install deps
  composer install

  # run
  php runner.php ../resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "php tests failed"
      exit 1
  fi
  echo "php tests finished"
else
  echo "skipping php tests"
fi

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'nodejs-pg'* ]]; then
  echo "starting nodejs tests with the pg driver"

  # check if nodejs is installed
  if ! command -v node &> /dev/null
  then
      echo "node.js could not be found! Please install nodejs or exclude nodejs tests"
      exit 1
  fi

  echo "$base_dir/compat/src/test/nodejs-pg"
  cd "$base_dir/compat/src/test/nodejs-pg" || exit

  # install deps
  npm install

  # run
  node runner.js ../resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "nodejs pg tests failed"
      exit 1
  fi
  echo "nodejs pg driver tests finished"
else
  echo "skipping nodejs pg driver tests"
fi

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'nodejs-postgres'* ]]; then
  echo "starting nodejs tests with the postgres driver"

  # check if nodejs is installed
  if ! command -v node &> /dev/null
  then
      echo "node.js could not be found! Please install nodejs or exclude nodejs tests"
      exit 1
  fi

  echo "$base_dir/compat/src/test/nodejs-postgres"
  cd "$base_dir/compat/src/test/nodejs-postgres" || exit

  # install deps
  npm install

  # run
  node runner.js ../resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "nodejs postgres driver tests failed"
      exit 1
  fi
  echo "nodejs postgres driver tests finished"
else
  echo "skipping nodejs postgres driver tests"
fi

if [[ $CLIENTS == 'ALL' || $CLIENTS == *'golang-pgx'* ]]; then
  echo "starting golang tests with the pgx driver"

  echo "$base_dir/compat/src/test/golang"
  cd "$base_dir/compat/src/test/golang" || exit

  go build
  export DB_DRIVER="pgx"
  # run
  ./gorunner ../resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "golang pgx driver tests failed"
      exit 1
  fi
  echo "golang pgx driver tests finished"
else
  echo "skipping golang pgx driver tests"
fi
if [[ $CLIENTS == 'ALL' || $CLIENTS == *'golang-pq'* ]]; then
  echo "starting golang tests with the pq driver"
  export DB_DRIVER="pq"
  echo "$base_dir/compat/src/test/golang"
  cd "$base_dir/compat/src/test/golang" || exit

  go build

  # run
  ./gorunner ../resources/test_cases.yaml
  if [ $? -ne 0 ]; then
      echo "golang pq driver tests failed"
      exit 1
  fi
  echo "golang pq driver tests finished"
else
  echo "skipping golang pq driver tests"
fi