This is the default directory for the SQL COPY command and the read_parquet() SQL function.

Drop files here to import data into QuestDB.

You can change the default directory by setting the cairo.sql.copy.root property in server.conf.

See:
    CSV import: https://questdb.io/docs/guides/import-csv/
    Reading external Parquet files: https://questdb.io/docs/reference/function/parquet/
---

This directory also contains a demo parquet file: trades.parquet.
You can use to test the read_parquet() SQL function.
Open the QuestDB web console and run the following SQL query: SELECT * FROM read_parquet('trades.parquet');

You can run aggregation queries on the data in the Parquet file. For example:
SELECT MIN(price), MAX(price)
FROM read_parquet('trades.parquet');

Data in the demo Parquet file are sorted by timestamp. This allows for efficient time-series queries,
but you need to specify the timestamp column when reading the Parquet file. For example, to sample the data by 1 day:
WITH trades AS (
    (SELECT * FROM read_parquet('trades.parquet')) TIMESTAMP(timestamp)
)
SELECT timestamp, MIN(price), MAX(price), FIRST(price) AS open, LAST(price) AS close FROM trades
SAMPLE BY 1d;

And last but not least, you can create a regular table and import the data from the Parquet file into it:
CREATE TABLE trades AS
(SELECT symbol::symbol AS symbol, side::symbol AS side, price, amount, timestamp FROM read_parquet('trades.parquet'))
TIMESTAMP(timestamp)
PARTITION BY MONTH WAL;