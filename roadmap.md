__lang__

- ~~subqueries~~
- simple aggregation (sum, count, avg etc)
- simple resampling (where aggregation function produces single row of values)
- multi-row aggregation
- multi-row resampling
- ~~classic sql joins (hash, nested loops)~~
- ~~latest record by secondary entity attribute attribute~~
    (e.g. select latest order for all customers in group X, where
    orders is a time series and customers is dimension and "group" is a field of customer)
- time joins (merge)
- as of joins
- ordering
- top x rows
- bottom x rows

__server__

- file upload handling (multipart form parsing)
- file upload handling from curl (100-continue recognition)
- query execution
- result set serialization (binary, json? - for javascript grid or chart)
- flow control on file upload (park upload if client is not sending file)
- flow control on query executions (do not execute until client is ready to read, park streaming if client is not ready to read)
- manage parked uploads and downloads (timeout with resource cleanup)
- C layer for windows (io completion ports)
- C layer for linux (epoll)
- C layer for mac/bsd (kqueue)

__misc__

- collectd protocol support and console administration
- circular memory buffer for "bottom x rows"

__clients__

- client for python
- client for R
- client for java
- build fail-over in all clients
- grafana integration
- web query UI (ipython style)

__core__

- ORM for query results
- cache for compiled queries
