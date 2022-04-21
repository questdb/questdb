# QuestDB command line utils

### TxSerializer

Serializes binary `_txn` file to / from readable JSON format. Primary usage to
investigate storage issues

#### Usage

```
io.questdb.cliutil.TxSerializer -d <json_path> | -s <json_path> <txn_path>
```

- `-d` option prints contents of `_txn` file to std output in JSON format
- `-s` option transforms existing JSON file into binary \_txn format

#### Examples

```bash
java -cp utils.jar io.questdb.cliutil.TxSerializer -d /questdb-root/db/trades-COINBASE/_txn > /questdb-root/db/trades-COINBASE/txn.json

java -cp utils.jar io.questdb.cliutil.TxSerializer -s /questdb-root/db/trades-COINBASE/txn.json /questdb-root/db/trades-COINBASE/_txnCopy
```

### Rebuild index

Rebuilds indexes for a table

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

Rebuilds indexes for a table

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

## Build Utils project

To build single jar with dependencies run

```bash
mvn clean package
```
