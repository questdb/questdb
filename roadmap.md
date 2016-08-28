__SQL implementation__

- [x] subqueries
- [x] support for NULL and NaN values in filters
- [x] simple aggregation (sum, count, avg etc)
        (aggregation does not require "group by" clause. Compiler works out
        key fields names automatically, e.g. 'select x, sum(y) from A' is
        equivalent to 'select x, sum(y) from A group by x')
- [x] simple resampling (where aggregation function produces single row of values)
- [x] classic sql joins (hash, nested loops)
- [x] latest record by secondary entity attribute attribute
    (e.g. select latest order for all customers in group X, where
    orders is a time series and customers is dimension and "group" is a field of customer)
- [ ] time joins (merge)
- [x] as of joins (trade asof join quote on trade.ccy = quote.ccy)
- [x] ordering
- [x] top x rows (select ... limit low,high can be used for paging)
- [ ] bottom x rows
- [x] query parameters
- [x] support for comments (both block /* */ and line --)
- [x] analytic functions
- [x] subquery optimiser
- [x] order by optimiser
- [ ] NEW distributed queries
- [ ] NEW pivot tables
- [ ] __DDL - create journal (in progress)__
- [ ] __DDL - copy journal (in progress)__
- [ ] DDL - add/remove column
- [ ] DDL - delete journal

__server__

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
- [ ] __authentication and authorization__
- [ ] MySQL wire protocol implementation
- [x] sql result export to delimited format

__monitoring__

- [ ] session details
- [ ] active queries
- [ ] memory usage
- [ ] open files

__misc__

- [ ] collectd protocol support and console administration
- [ ] circular memory buffer for "bottom x rows"
- [x] error handling (releasing resources allocated to partially constructed query plans)
- [x] Linux/OSX start script
- [x] Windows service

__community__

- [x] __project web site__
- [x] example data set
- [ ] __video tutorial (in progress)__
- [ ] __demo server__
- [ ] documentation template

__clients__

- [ ] grafana integration

__Web UI__

- [x] query execution and result display
- [x] grid styling
- [x] error reporting and interaction with query editor
- [x] grid virtualisation
- [x] file import UI
- [x] copy SQL export url to clipboard for use in other applications
- [ ] data visualisation

__core__

- [ ] ORM for query results
- [x] cache for compiled queries
