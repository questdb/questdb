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

topics=quickstart-events2
insert.mode=insert
dialect.name=PostgreSqlDatabaseDialect
pk.mode=none
auto.create=true
```
