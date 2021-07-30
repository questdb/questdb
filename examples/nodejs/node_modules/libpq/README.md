# node-libpq

[![Build Status](https://travis-ci.org/brianc/node-libpq.svg?branch=master)](https://travis-ci.org/brianc/node-libpq)

Node native bindings to the PostgreSQL [libpq](http://www.postgresql.org/docs/9.3/interactive/libpq.html) C client library.  This module attempts to mirror _as closely as possible_ the C API provided by libpq and provides the absolute minimum level of abstraction.  It is intended to be extremely low level and allow you the same access as you would have to libpq directly from C, except in node.js! The obvious trade-off for being "close to the metal" is having to use a very "c style" API in JavaScript.

If you have a good understanding of libpq or used it before hopefully the methods within node-libpq will be familiar; otherwise, you should probably spend some time reading [the official libpq C library documentation](http://www.postgresql.org/docs/9.3/interactive/libpq.html) to become a bit familiar. Referencing the libpq documentation directly should also provide you with more insight into the methods here. I will do my best to explain any differences from the C code for each method.

I am also building some [higher level abstractions](https://github.com/brianc/node-pg-native) to eventually replace the `pg.native` portion of node-postgres.  They should help as reference material.

This module relies heavily on [nan](https://github.com/rvagg/nan) and wouldn't really be possible without it. Mucho thanks to the node-nan team.

## install

You need libpq installed & the `pg_config` program should be in your path.  You also need [node-gyp](https://github.com/TooTallNate/node-gyp) installed.

```bash
$ npm install libpq
```

> Note: for Node.js equal or greater to version 10.16.0 you need to have at least `OpenSSL 1.1.1` installed.

## use

```js
var Libpq = require('libpq');
var pq = new Libpq();
```

## API

### connection functions

Libpq provides a few different connection functions, some of which are "not preferred" anymore.  I've opted to simplify this interface a bit into a single __async__ and single __sync__ connnection function.  The function accepts an  connection string formatted as outlined [in this documentation in section 31.1.1](http://www.postgresql.org/docs/9.3/static/libpq-connect.html). If the parameters are not supplied, libpq will automatically use environment variables, a pgpass file, and other options.  Consult the libpq documentation for a better rundown of all the ways it tries to determine your connection parameters.

I personally __always__ connect with environment variables and skip supplying the optional `connectionParams`.  Easier, more 12 factor app-ish, and you never risk hard coding any passwords. YMMV. :smile:

##### `pq.connect([connectionParams:string], callback:function)`

Asyncronously attempts to connect to the postgres server.

- `connectionParams` is an optional string
- `callback` is mandatory. It is called when the connection has successfully been established.

__async__ Connects to a PostgreSQL backend server process.

This function actually calls the `PQconnectdb` blocking connection method in a background thread within node's internal thread-pool. There is a way to do non-blocking network I/O for some of the connecting with libpq directly, but it still blocks when your local file system looking for config files, SSL certificates, .pgpass file, and doing possible dns resolution.  Because of this, the best way to get _fully_ non-blocking is to juse use `libuv_queue_work` and let node do it's magic and so that's what I do.  This function _does not block_.

##### `pq.connectSync([connectionParams:string])`

Attempts to connect to a PostgreSQL server. __BLOCKS__ until it either succeedes, or fails.  If it fails it will throw an exception.

- `connectionParams` is an optional string

##### `pq.finish()`

Disconnects from the backend and cleans up all memory used by the libpq connection.

### Connection Status Functions

##### `pq.errorMessage():string`

Retrieves the last error message from the connection.  This is intended to be used after most functions which return an error code to get more detailed error information about the connection.  You can also check this _before_ issuing queries to see if your connection has been lost.

##### `pq.socket():int`

Returns an int representing the file descriptor for the socket used internally by the connection

### Sync Command Execution Functions

##### `pq.exec(commandText:string)`

__sync__ sends a command to the backend and blocks until a result is received.

- `commandText` is a required string of the query.

##### `pq.execParams(commandText:string, parameters:array[string])`

__snyc__ sends a command and parameters to the backend and blocks until a result is received.

- `commandText` is a required string of the query.
- `parameters` is a required array of string values corresponding to each parameter in the commandText.

##### `pq.prepare(statementName:string, commandText:string, nParams:int)`
__sync__ sends a named statement to the server to be prepared for later execution. blocks until a result from the prepare operation is received.

- `statementName` is a required string of name of the statement to prepare.
- `commandText` is a required string of the query.
- `nParams` is a count of the number of parameters in the commandText.

##### `pq.execPrepared(statementName:string, parameters:array[string])`
__sync__ sends a command to the server to execute a previously prepared statement. blocks until the results are returned.

- `statementName` is a required string of the name of the prepared statement.
- `parameters` are the parameters to pass to the prepared statement.

### Async Command Execution Functions

In libpq the async command execution functions _only_ dispatch a request to the backend to run a query.  They do not start result fetching on their own.  Because libpq is a C api there is a somewhat complicated "dance" to retrieve the result information in a non-blocking way.  node-libpq attempts to do as little as possible to abstract over this; therefore, the following functions are only part of the story.  For a complete tutorial on how to dispatch & retrieve results from libpq in an async way you can [view the complete approach here](https://github.com/brianc/node-pg-native/blob/master/index.js#L105)

##### `pq.sendQuery(commandText:string):boolean`
__async__ sends a query to the server to be processed.

- `commandText` is a required string containing the query text.

Returns `true` if the command was sent succesfully or `false` if it failed to send.

##### `pq.sendQueryParams(commandText:string, parameters:array[string]):boolean`
__async__ sends a query and to the server to be processed.

- `commandText` is a required string containing the query text.
- `parameters` is an array of parameters as strings used in the parameterized query.

Returns `true` if the command was sent succesfully or `false` if it failed to send.

##### `pq.sendPrepare(statementName:string, commandText:string, nParams:int):boolean`
__async__ sends a request to the backend to prepare a named statement with the given name.

- `statementName` is a required string of name of the statement to prepare.
- `commandText` is a required string of the query.
- `nParams` is a count of the number of parameters in the commandText.

Returns `true` if the command was sent succesfully or `false` if it failed to send.

##### `pq.sendQueryPrepared(statementName:string, parameters:array[string]):boolean`
__async__ sends a request to execute a previously prepared statement.

- `statementName` is a required string of the name of the prepared statement.
- `parameters` are the parameters to pass to the prepared statement.

##### `pq.getResult():boolean`
Parses received data from the server into a `PGresult` struct and sets a pointer internally to the connection object to this result.  __warning__: this function will __block__ if libpq is waiting on async results to be returned from the server.  Call `pq.isBusy()` to determine if this command will block.

Returns `true` if libpq was able to read buffered data & parse a result object.  Returns `false` if there are no results waiting to be parsed.  Generally doing async style queries you'll call this repeadedly until it returns false and then use the result accessor methods to pull results out of the current result set.

### Result accessor functions

After a command is run in either sync or async mode & the results have been received, node-libpq stores the results internally and provides you access to the results via the standard libpq methods.  The difference here is libpq will return a pointer to a PGresult structure which you access via libpq functions, but node-libpq stores the most recent result within itself and passes the opaque PGresult structure to the libpq methods.  This is to avoid passing around a whole bunch of pointers to unmanaged memory and keeps the burden of properly allocating and freeing memory within node-libpq.

##### `pq.resultStatus():string`

Returns either `PGRES_COMMAND_OK` or `PGRES_FATAL_ERROR` depending on the status of the last executed command.

##### `pq.resultErrorMessage():string`

Retrieves the error message from the result.  This will return `null` if the result does not have an error.

##### `pq.resultErrorFields():object`

Retrieves detailed error information from the current result object. Very similar to `PQresultErrorField()` except instead of passing a fieldCode and retrieving a single field, retrieves all fields from the error at once on a single object.  The object returned is a simple hash, _not_ an instance of an error object.  Example: if you wanted to access `PG_DIAG_MESSAGE_DETAIL` you would do the following:

```js
console.log(pq.errorFields().messageDetail)
```

##### `pq.clear()`

Manually frees the memory associated with a `PGresult` pointer.  Generally this is called for you, but if you absolutely want to free the pointer yourself, you can.

##### `pq.ntuples():int`

Retrieve the number of tuples (rows) from the result.

##### `pq.nfields():int`

Retrieve the number of fields (columns) from the result.

##### `pq.fname(fieldNumber:int):string`

Retrieve the name of the field (column) at the given offset. Offset starts at 0.

##### `pq.ftype(fieldNumber:int):int`

Retrieve the `Oid` of the field (column) at the given offset. Offset starts at 0.

##### `pq.getvalue(tupleNumber:int, fieldNumber:int):string`

Retrieve the text value at a given tuple (row) and field (column) offset. Both offsets start at 0.  A null value is returned as the empty string `''`.

##### `pq.getisnull(tupleNumber:int, fieldNumber:int):boolean`

Returns `true` if the value at the given offsets is actually `null`.  Otherwise returns `false`.  This is because `pq.getvalue()` returns an empty string for both an actual empty string and for a `null` value.  Weird, huh?

##### `pq.cmdStatus():string`

Returns the status string associated with a result.  Something akin to `INSERT 3 0` if you inserted 3 rows.

##### `pq.cmdTuples():string`

Returns the number of tuples (rows) affected by the command. Even though this is a number, it is returned as a string to mirror libpq's behavior.

### Async socket access

These functions don't have a direct match within libpq.  They exist to allow you to monitor the readability or writability of the libpq socket based on your platforms equivilant to `select()`.  This allows you to perform async I/O completely from JavaScript.

##### `pq.startReader()`

This uses libuv to start a read watcher on the socket open to the backend.  As soon as this socket becomes readable the `pq` instance will emit a `readable` event.  It is up to you to call `pq.consumeInput()` one or more times to clear this read notification or it will continue to emit read events over and over and over.  The exact flow is outlined [here] under the documentation for `PQisBusy`.

##### `pq.stopReader()`

Tells libuv to stop the read watcher on the connection socket.

##### `pq.writable(callback:function)`

Call this to make sure the socket has flushed all data to the operating system.  Once the socket is writable, your callback will be called.  Usefully when using `PQsetNonBlocking` and `PQflush` for async writing.

### More async methods

These are all documented in detail within the [libpq documentation](http://www.postgresql.org/docs/9.3/static/libpq-async.html) and function almost identically.

##### `pq.consumeInput():boolean`

Reads waiting data from the socket.  If the socket is not readable and you call this it will __block__ so be careful and only call it within the `readable` callback for the most part.

Returns `true` if data was read.  Returns `false` if there was an error.  You can access error details with `pq.errorMessage()`.

##### `pq.isBusy():boolean`

Returns `true` if calling `pq.consumeInput()` would block waiting for more data.  Returns `false` if all data has been read from the socket.  Once this returns `false` it is safe to call `pq.getResult()`

##### `pq.setNonBlocking(nonBlocking:boolean):boolean`

Toggle the socket blocking on _write_.  Returns `true` if the socket's state was succesfully toggled.  Returns `false` if there was an error.

- `nonBlocking` is `true` to set the connection to use non-blocking writes. `false` to use blocking writes.

##### `pq.flush():int`

Flushes buffered data to the socket.  Returns `1` if socket is not write-ready at which case you should call `pq.writable` with a callback and wait for the socket to be writable and then call `pq.flush()` again.  Returns `0` if all data was flushed.  Returns `-1` if there was an error.

### listen/notify

##### `pq.notifies():object`

Checks for `NOTIFY` messages that have come in.  If any have been received they will be in the following format:

```js
var msg = {
  relname: 'name of channel',
  extra: 'message passed to notify command',
  be_pid: 130
}
```

### COPY IN/OUT

##### `pq.putCopyData(buffer:Buffer):int`

After issuing a successful command like `COPY table FROM stdin` you can start putting buffers directly into the databse with this function.

- `buffer` Is a required node buffer of text data such as `Buffer('column1\tcolumn2\n')`

Returns `1` if sent succesfully. Returns `0` if the command would block (only if you have called `pq.setNonBlocking(true)`). Returns `-1` if there was an error sending the command.

##### `pq.putCopyEnd([errorMessage:string])`

Signals the backed your copy procedure is complete.  If you pass `errorMessage` it will be sent to the backend and effectively cancel the copy operation.

- `errorMessage` is an _optional_ string you can pass to cancel the copy operation.

Returns `1` if sent succesfully. Returns `0` if the command would block (only if you have called `pq.setNonBlocking(true)`). Returns `-1` if there was an error sending the command.


##### `pq.getCopyData(async:boolean):Buffer or int`

After issuing a successfuly command like `COPY table TO stdout` gets copy data from the connection.

Returns a node buffer if there is data available.

Returns `0` if the copy is still in progress (only if you have called `pq.setNonBlocking(true)`). Returns `-1` if the copy is completed. Returns `-2` if there was an error.

- `async` is a boolean. Pass `false` to __block__ waiting for data from the backend. _defaults to `false`_

### Misc Functions

##### `pq.escapeLiteral(input:string):string`

Exact copy of the `PQescapeLiteral` function within libpq.  Requires an established connection but does not perform any I/O.

##### `pq.escapeIdentifier(input:string):string`

Exact copy of the `PQescapeIdentifier` function within libpq.  Requires an established connection but does not perform any I/O.

##### `pq.cancel():true -or- string`

Issues a request to cancel the currently executing query _on this instance of libpq_.  Returns `true` if the cancel request was sent.  Returns a `string` error message if the cancel request failed for any reason. The string will contain the error message provided by libpq.

##### `pq.serverVersion():number`

Returns the version of the connected PostgreSQL backend server as a number.

## testing

```sh
$ npm test
```

To run the tests you need a PostgreSQL backend reachable by typing `psql` with no connection parameters in your terminal. The tests use [environment variables](http://www.postgresql.org/docs/9.3/static/libpq-envars.html) to connect to the backend. 

An example of supplying a specific host the tests:

```sh
$ PGHOST=blabla.mydatabasehost.com npm test
```


## license

The MIT License (MIT)

Copyright (c) 2014 Brian M. Carlson

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
