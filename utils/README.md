# QuestDB command line utils

### TxSerializer

Serializes binary `_txn` file to / from readable JSON format. Primary usage to investigate storage issues

Usage 

```
io.questdb.cliutil.TxSerializer -d <json_path> | -s <json_path> <txn_path> 
```

- `-d` option prints contents of `_txn` file to std output in JSON format
- `-s` option transforms existing JSON file into binary _txn format