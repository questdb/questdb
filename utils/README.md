# QuestDB command line utils

### TxSerializer

Serializes binary `_txn` file to / from readable JSON format. Primary usage to
investigate storage issues

#### Usage

```
io.questdb.cliutil.TxSerializer -d <txn_path> | -s <json_path> <txn_path>
```

- `-d` option prints contents of `_txn` file to std output in JSON format
- `-s` option transforms existing JSON file into binary \_txn format

#### Examples

```bash
java -cp utils.jar io.questdb.cliutil.TxSerializer -d /questdb-root/db/trades-COINBASE/_txn > /questdb-root/db/trades-COINBASE/txn.json

java -cp utils.jar io.questdb.cliutil.TxSerializer -s /questdb-root/db/trades-COINBASE/txn.json /questdb-root/db/trades-COINBASE/_txnCopy
```

### Rebuild index

Rebuilds table indexes

#### Usage

```
io.questdb.cliutil.RebuildIndex <table_path> [-p <partition_name>] [-c <column_name>]
```

- `<table_path>` full path to the table
- `-c` column name, optional. If omitted, all indexed columns will have indexes
  rebuilt
- `-p` optional partition name. If omitted, all partitions will be affected

#### Examples

```bash
java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE

java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE -c symbol

java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE -p 2022-03-21

java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE -p 2022-03-21 -c symbol
```

### Rebuild String column index `.i` file

Rebuilds String column `.i` file from `.d` file, helpful when `.i` file is corrupted

#### Usage

```
io.questdb.cliutil.RecoverVarIndex <table_path> [-p <partition_name>] [-c <column_name>]
```

- `<table_path>` full path to the table
- `-c` column name, optional. If omitted, all string columns will have `.i` file
  rebuild
- `-p` optional partition name. If omitted, all partitions will be affected

#### Examples

```bash
java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE

java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE -c stringColumn

java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE -p 2022-03-21

java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE -p 2022-03-21 -c stringColumn
```

### Copy table from one instance to another using Postgres wire to read and ILP to write

Copies all the data from one QuestDB instance to another. Uses Postgres wire to select the data and ILP to insert it.
Useful to migrate data to the running instance.

#### Usage

```
io.questdb.cliutil.Table2Ilp -d <destination_table_name> -dc <destination_ilp_host_port> -s <source_select_query> -sc <source_pg_connection_string>
                               [-sts <timestamp_column>] [-sym <symbol_columns>] [-dauth <ilp_auth_key:ilp_auth_token>] [-dtls]
```

- `-d` destination table name
- `-dilp` destination ILP connection string, e.g. `https::addr=localhost:9000;username=admin;password=quest;`
- `-s` source select query, e.g. `trades` or `trades WHERE timestamp in '2021-01'`
- `-sc` source connection string, e.g. `jdbc:pgsql://localhost:8812/qdb`
- `-sts` source designated timestamp column name, defaults to `timestamp`
- `-sym` comma separated list of symbol columns, e.g. `symbol,exchange`

#### Examples

```bash
java -cp utils.jar io.questdb.cliutil.Table2Ilp -d trades -dilp "https::addr=localhost:9000;username=admin;password=quest;" -s "trades WHERE start_time in '2022-06'" \ 
     -sc "jdbc:postgresql://localhost:9812/qdb?user=account&password=secret&ssl=false" \
     -sym "ticker,exchagne" -sts start_time

```

## Build Utils project

To build single jar with dependencies run

```bash
mvn clean package
```
