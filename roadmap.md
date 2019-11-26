__SQL__

- [ ] Authorization framework:
        granular authorization framework to govern data access at table, column and SQL functional level
- [ ] Analytic functions
- [ ] Views
- [ ] Incremental queries: reuse result of previously users secure queries to reduce calculation effort
- [ ] true - parallel query execution
- [ ] low latency RPC (remote async query execution)
- [ ] column type change
- [ ] drop table partition
- [ ] replace table partition with aggregated data: downsample historical data to reduce data volume as significance of data diminishes
- [ ] pivot table
- [ ] parquet import
- [ ] AVRO import
- [ ] Cancel active query: lightweight method for cancelling loops that have negligeable impact on query performance
- [ ] JSON search
- [ ] geospatial search
- [ ] ACID over multiple tables 

- [x] subqueries
- [x] support for NULL and NaN values in filters
- [x] simple aggregation (sum, count, avg etc)
        (aggregation does not require "group by" clause. Compiler works out
        key fields names automatically, e.g. 'select x, sum(y) from A' is
        equivalent to 'select x, sum(y) from A group by x')
- [x] simple resampling (where aggregation function produces single row of values)
- [x] classic sql joins (hash, nested loops)
- [x] Latest record by secondary entity attribute attribute: select latest order for all customers in group X, where
    orders is a time series and customers is dimension and "group" is a field of customer
- [x] time joins (merge)
- [x] as of joins (trade asof join quote on trade.ccy = quote.ccy)
- [x] ordering
- [x] top x rows (select ... limit low,high can be used for paging)
- [x] bottom x rows
- [x] query parameters
- [x] support for comments (both block /* */ and line --)
- [x] analytic functions
- [x] subquery optimiser
- [x] order by optimiser
- [x] DDL - create table
- [x] DDL - copy journal (sample syntax: create table x as (y where a=1 order by b))
- [x] DDL - add/remove column
- [x] DDL - drop table
- [x] data generation functions
- [x] COPY from implementation


__HTTP server__

- [ ] authentication and authorization
- [ ] queued DDL execution
        whenever table writer is needed and not available due to normal race condition server should
        queue the query and attempt to execute autonomously and periodcally
- [ ] choice of export formats
- [ ] query parameter support
- [ ] grafana integration
- [x] file upload handling (multipart form parsing)
- [x] file upload handling from curl (100-continue recognition)
- [x] query execution
- [x] JSON result set serialization
- [x] flow control on file upload (park upload if client is not sending file)
- [x] flow control on query executions (do not execute until client is ready to read, park streaming if client is not ready to read)
- [x] SSL support
- [x] http compression
- [x] high-performance logging
- [x] manage parked uploads and downloads (timeout with resource cleanup)
- [x] C layer for windows
- [x] C layer for linux (epoll)
- [x] C layer for mac/bsd (kqueue)
- [x] sql result export to delimited format

__PostgresSQL wire server__

- [ ] insert execution with parameters 
- [ ] psql: support metadata queries 
- [x] query execution
- [x] query parameters
- [x] DDL execution

__Influx wire server__
- [ ] TCP server
- [x] Protocol parser
- [x] UDP server

__monitoring__

- [ ] session details
- [ ] active queries
- [ ] memory usage
- [ ] open files
