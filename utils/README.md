Here's the corrected version of the QuestDB command line utils documentation:

# QuestDB Command Line Utils

## TxSerializer

Serializes binary `_txn` file to/from readable JSON format. Primary usage is to investigate storage issues.

### Usage

```
io.questdb.cliutil.TxSerializer -d <txn_path> | -s <json_path> <txn_path>
```

- `-d` option prints contents of `_txn` file to standard output in JSON format
- `-s` option transforms existing JSON file into binary `_txn` format

### Examples

```bash
java -cp utils.jar io.questdb.cliutil.TxSerializer -d /questdb-root/db/trades-COINBASE/_txn > /questdb-root/db/trades-COINBASE/txn.json

java -cp utils.jar io.questdb.cliutil.TxSerializer -s /questdb-root/db/trades-COINBASE/txn.json /questdb-root/db/trades-COINBASE/_txnCopy
```

## Rebuild Index

Rebuilds table indexes.

### Usage

```
io.questdb.cliutil.RebuildIndex <table_path> [-p <partition_name>] [-c <column_name>]
```

- `<table_path>` is the full path to the table
- `-c` specifies the column name. If omitted, all indexed columns will have indexes rebuilt
- `-p` specifies the optional partition name. If omitted, all partitions will be affected

### Examples

```bash
java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE

java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE -c symbol

java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE -p 2022-03-21

java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE -p 2022-03-21 -c symbol
```

## Rebuild String Column Index `.i` File

Rebuilds the String column `.i` file from the `.d` file, helpful when the `.i` file is corrupted.

### Usage

```
io.questdb.cliutil.RecoverVarIndex <table_path> [-p <partition_name>] [-c <column_name>]
```

- `<table_path>` is the full path to the table
- `-c` specifies the column name. If omitted, all string columns will have their `.i` files rebuilt
- `-p` specifies the optional partition name. If omitted, all partitions will be affected

### Examples

```bash
java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE

java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE -c stringColumn

java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE -p 2022-03-21

java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE -p 2022-03-21 -c stringColumn
```

## Copy Table from One Instance to Another Using Postgres Wire to Read and ILP to Write

Copies all the data from one QuestDB instance to another. Uses Postgres wire to select the data and ILP to insert it. Useful for migrating data to a running instance.

### Usage

```
io.questdb.cliutil.Table2Ilp -d <destination_table_name> -dc <destination_ilp_host_port> -s <source_select_query> -sc <source_pg_connection_string>
                               [-sts <timestamp_column>] [-sym <symbol_columns>] [-dauth <ilp_auth_key:ilp_auth_token>] [-dtls]
```

- `-d` specifies the destination table name
- `-dc` specifies the destination ILP host and port, e.g., `localhost:9009`
- `-s` specifies the source select query, e.g., `trades` or `trades WHERE timestamp in '2021-01'`
- `-sc` specifies the source connection string, e.g., `jdbc:pgsql://localhost:8812/qdb`
- `-sts` specifies the source designated timestamp column name, defaults to `timestamp`
- `-sym` specifies a comma-separated list of symbol columns, e.g., `symbol,exchange`
- `-dauth` specifies the ILP key and authentication token, e.g., `admin:GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0`
- `-dtls` specifies to use TLS for the ILP connection. False by default.

### Examples

```bash
java -cp utils.jar io.questdb.cliutil.Table2Ilp -d trades -dc localhost:9009 -s "trades WHERE start_time in '2022-06'" \
     -sc "jdbc:postgresql://localhost:9812/qdb?user=account&password=secret&ssl=false" \
     -sym "ticker,exchagne" -sts start_time
```

## Build Utils Project

To build a single JAR with dependencies, run:

```bash
mvn clean package
```
