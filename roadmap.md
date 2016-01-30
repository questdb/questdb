__lang__

- [x] subqueries
- [x] support for NULL and NaN values in filters
- [x] simple aggregation (sum, count, avg etc)
        (aggregation does not require "group by" clause. Compiler works out
        key fields names automatically, e.g. 'select x, sum(y) from A' is
        equivalent to 'select x, sum(y) from A group by x')
- [x] simple resampling (where aggregation function produces single row of values)
- [ ] multi-row aggregation
- [ ] multi-row resampling
- [x] classic sql joins (hash, nested loops)
- [x] latest record by secondary entity attribute attribute
    (e.g. select latest order for all customers in group X, where
    orders is a time series and customers is dimension and "group" is a field of customer)
- [ ] time joins (merge)
- [x] as of joins (trade asof join quote on trade.ccy = quote.ccy)
- [ ] ordering
- [x] top x rows (select ... limit low,high can be used for paging)
- [ ] bottom x rows

__server__

- [x] file upload handling (multipart form parsing)
- [x] file upload handling from curl (100-continue recognition)
- [x] query execution
- [ ] __JSON result set serialization (in progress)__
- [x] flow control on file upload (park upload if client is not sending file)
- [x] flow control on query executions (do not execute until client is ready to read, park streaming if client is not ready to read)
- [x] SSL support
- [x] http compression
- [ ] __high-performance logging (in progress)__
- [ ] manage parked uploads and downloads (timeout with resource cleanup)
- [ ] C layer for windows (io completion ports)
- [ ] C layer for linux (epoll)
- [ ] __C layer for mac/bsd (kqueue) (in progress)__
- [ ] MySQL wire protocol implementation

__misc__

- [ ] collectd protocol support and console administration
- [ ] circular memory buffer for "bottom x rows"

__clients__

- [ ] grafana integration
- [ ] __web query UI (in progress)__

__core__

- [ ] ORM for query results
- [x] cache for compiled queries
