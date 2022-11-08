# How to set up kafka PostgreSQL connector for development

You will need the following:

- PostgreSQL binary
- Kafka binary
- Kafka Connect JDBC
- PostgreSQL JDBC driver

## PostgreSQL binary

Create new PostgreSQL database

```shell script
initdb -D "C:\pgdata"
```

Start PostgreSQL

```shell script
pg_ctl -D "C:\pgdata" start
```

Connect to PostgreSQL

```shell script
psql -d postgres
```

## Kafka binary

Start zookeeper from kafka home directory. All paths are relative.

```shell script
bin/windows/zookeeper-server-start.bat  config/zookeeper.properties
```

Start Kafka server

```shell script
bin/windows/kafka-server-start.bat  config/server.properties
```

Start kafka connect

```shell script
bin/windows/connect-standalone.bat config/connect-standalone.properties config/connect-jdbc.properties
```

`connect-jdbc.properties`:

```properties
name=local-jdbc-sink
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
connection.url=jdbc:postgresql://127.0.0.1:8812/qdb?useSSL=false
connection.user=admin
connection.password=quest

topics=quickstart-events
insert.mode=insert
dialect.name=PostgreSqlDatabaseDialect
pk.mode=none
auto.create=true
```

To publish a message

```shell
bin/windows/kafka-console-producer.bat --topic quickstart-events2 --bootstrap-server localhost:9092
bin/windows/kafka-console-producer.bat --topic spot --bootstrap-server localhost:9092
```

Paste this message (as one line) to create a table. The table name will be topic
used in the kafka-console-producer topic

<!-- prettier-ignore -->
```json
{    "schema": {        "type": "struct",        "fields": [            {                "type": "boolean",                "optional": false,               "field": "flag"            },            {                "type": "int8",                "optional": false,                "field": "id8"           },           {                "type": "int16",                "optional": false,                "field": "id16"            },            {                "type":"int32",                "optional": false,                "field": "id32"            },          {                  "type": "int64",               "optional": false,                "field": "id64"            },            {                "type": "float",                "optional": false,                "field": "idFloat"            },            {                "type": "double",                "optional": false,                "field": "idDouble"            },              {                "type": "string",                "optional": true,                "field": "msg"            }      ],        "optional": false,        "name": "msgschema"    },    "payload": {        "flag": false,        "id8": 222,        "id16": 222,        "id32": 222,        "id64": 222,        "idFloat": 222.0,        "idDouble": 333.0,               "msg": "hi"  }}
```

json to reproduce issue with the timestamp

<!-- prettier-ignore -->
```json
{    "schema": {        "type": "struct",        "fields": [{            "type": "int32",            "optional": true,            "field": "c1"        }, {            "type": "string",            "optional": true,            "field": "c2"        }, {            "type": "int64",            "optional": false,            "name": "org.apache.kafka.connect.data.Timestamp",            "version": 1,            "field": "create_ts"        }, {            "type": "int64",            "optional": false,            "name": "org.apache.kafka.connect.data.Timestamp",            "version": 1,            "field": "update_ts"        }],        "optional": false,        "name": "foobar"    },    "payload": {        "c1": 10000,        "c2": "bar",        "create_ts": 1501834166000,        "update_ts": 1501834166000    }}
```

this one is out of order for the above

<!-- prettier-ignore -->
```json
{    "schema": {        "type": "struct",        "fields": [{            "type": "int32",            "optional": true,            "field": "c1"        }, {            "type": "string",            "optional": true,            "field": "c2"        }, {            "type": "int64",            "optional": false,            "name": "org.apache.kafka.connect.data.Timestamp",            "version": 1,            "field": "create_ts"        }, {            "type": "int64",            "optional": false,            "name": "org.apache.kafka.connect.data.Timestamp",            "version": 1,            "field": "update_ts"        }],        "optional": false,        "name": "foobar"    },    "payload": {        "c1": 10000,        "c2": "bar",        "create_ts": 1501834166000,        "update_ts": 0    }}
```

<!-- prettier-ignore -->
```json
{    "schema": {        "type": "struct",        "fields": [{            "type": "int32",            "optional": true,            "field": "c1"        }, {            "type": "string",            "optional": true,            "field": "c2"        }, {            "type": "int32",            "optional": false,            "name": "org.apache.kafka.connect.data.Date",            "version": 1,            "field": "create_ts"        }, {            "type": "int64",            "optional": false,            "name": "org.apache.kafka.connect.data.Timestamp",            "version": 1,            "field": "update_ts"        }],        "optional": false,        "name": "foobar"    },    "payload": {        "c1": 10000,        "c2": "bar",        "create_ts": 1501834,        "update_ts": 1501834166    }}
```
