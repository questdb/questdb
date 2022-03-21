# QuestDB command line utils

### TxSerializer

Serializes binary `_txn` file to / from readable JSON format. Primary usage to
investigate storage issues

Usage

```
io.questdb.cliutil.TxSerializer -d <json_path> | -s <json_path> <txn_path>
```

- `-d` option prints contents of `_txn` file to std output in JSON format
- `-s` option transforms existing JSON file into binary \_txn format

Examples

```bash
java -cp utils.jar io.questdb.cliutil.TxSerializer -d /questdb-root/db/trades-COINBASE/_txn > /questdb-root/db/trades-COINBASE/txn.json

java -cp utils.jar io.questdb.cliutil.TxSerializer -s /questdb-root/db/trades-COINBASE/txn.json /questdb-root/db/trades-COINBASE/_txnCopy
```

### Rebuild index

Rebuilds indexes for a table

Usage

```
io.questdb.cliutil.RebuildIndex <table_path> [-p <partition_name>] [-c <column_name>]
```

- `<table_path>` full path to the table
- `-c` column name, optional. If omitted, all indexed columns will have indexes rebuilt
- `-p` option transforms existing JSON file into binary \_txn format

Examples

```bash
java -cp utils.jar io.questdb.cliutil.RebuildIndex /questdb-root/db/trades-COINBASE

java -cp utils.jar io.questdb.cliutil.RebuildIndex /questdb-root/db/trades-COINBASE -c symbol

java -cp utils.jar io.questdb.cliutil.RebuildIndex /questdb-root/db/trades-COINBASE -p 2022-03-21

java -cp utils.jar io.questdb.cliutil.RebuildIndex /questdb-root/db/trades-COINBASE -p 2022-03-21 -c symbol
```

### Rebuild variable column index

Rebuilds indexes for a table

Usage

```
io.questdb.cliutil.RebuildIndex <table_path> [-p <partition_name>] [-c <column_name>]
```

- `<table_path>` full path to the table
- `-c` column name, optional. If omitted, all indexed columns will have indexes rebuilt
- `-p` option transforms existing JSON file into binary \_txn format

Examples

```bash
java -cp utils.jar io.questdb.cliutil.RebuildIndex /questdb-root/db/trades-COINBASE

java -cp utils.jar io.questdb.cliutil.RebuildIndex /questdb-root/db/trades-COINBASE -c symbol

java -cp utils.jar io.questdb.cliutil.RebuildIndex /questdb-root/db/trades-COINBASE -p 2022-03-21

java -cp utils.jar io.questdb.cliutil.RebuildIndex /questdb-root/db/trades-COINBASE -p 2022-03-21 -c symbol
```

## Build Utils project

To build single jar with dependencies run

```bash
mvn clean package
```

