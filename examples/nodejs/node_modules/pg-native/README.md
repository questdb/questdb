# node-pg-native

[![Build Status](https://travis-ci.org/brianc/node-pg-native.svg?branch=master)](https://travis-ci.org/brianc/node-pg-native)

High performance native bindings between node.js and PostgreSQL via [libpq](https://github.com/brianc/node-libpq) with a simple API.

## install

You need PostgreSQL client libraries & tools installed. An easy way to check is to type `pg_config`. If `pg_config` is in your path, you should be good to go. If it's not in your path you'll need to consult operating specific instructions on how to go about getting it there.

Some ways I've done it in the past:

- On OS X: `brew install postgres`
- On Ubuntu/Debian: `apt-get install libpq-dev g++ make`
- On RHEL/CentOS: `yum install postgresql-devel`
- On Windows:
 1. Install Visual Studio C++ (successfully built with Express 2010). Express is free.
 2. Install PostgreSQL (`http://www.postgresql.org/download/windows/`)
 3. Add your Postgre Installation's `bin` folder to the system path (i.e. `C:\Program Files\PostgreSQL\9.3\bin`).
 4. Make sure that both `libpq.dll` and `pg_config.exe` are in that folder.

Afterwards `pg_config` should be in your path. Then...

```sh
$ npm i pg-native
```

## use

### async

```js
var Client = require('pg-native')

var client = new Client();
client.connect(function(err) {
  if(err) throw err

  //text queries
  client.query('SELECT NOW() AS the_date', function(err, rows) {
    if(err) throw err

    console.log(rows[0].the_date) //Tue Sep 16 2014 23:42:39 GMT-0400 (EDT)

    //parameterized statements
    client.query('SELECT $1::text as twitter_handle', ['@briancarlson'], function(err, rows) {
      if(err) throw err

      console.log(rows[0].twitter_handle) //@briancarlson
    })

    //prepared statements
    client.prepare('get_twitter', 'SELECT $1::text as twitter_handle', 1, function(err) {
      if(err) throw err

      //execute the prepared, named statement
      client.execute('get_twitter', ['@briancarlson'], function(err, rows) {
        if(err) throw err

        console.log(rows[0].twitter_handle) //@briancarlson

        //execute the prepared, named statement again
        client.execute('get_twitter', ['@realcarrotfacts'], function(err, rows) {
          if(err) throw err

          console.log(rows[0].twitter_handle) //@realcarrotfacts
          
          client.end(function() {
            console.log('ended')
          })
        })
      })
    })
  })
})

```

### sync

Because `pg-native` is bound to [libpq](https://github.com/brianc/node-libpq) it is able to provide _sync_ operations for both connecting and queries. This is a bad idea in _non-blocking systems_ like web servers, but is exteremly convienent in scripts and bootstrapping applications - much the same way `fs.readFileSync` comes in handy.

```js
var Client = require('pg-native')

var client = new Client()
client.connectSync()

//text queries
var rows = client.querySync('SELECT NOW() AS the_date')
console.log(rows[0].the_date) //Tue Sep 16 2014 23:42:39 GMT-0400 (EDT)

//parameterized queries
var rows = client.querySync('SELECT $1::text as twitter_handle', ['@briancarlson'])
console.log(rows[0].twitter_handle) //@briancarlson

//prepared statements
client.prepareSync('get_twitter', 'SELECT $1::text as twitter_handle', 1)

var rows = client.executeSync('get_twitter', ['@briancarlson'])
console.log(rows[0].twitter_handle) //@briancarlson

var rows = client.executeSync('get_twitter', ['@realcarrotfacts'])
console.log(rows[0].twitter_handle) //@realcarrotfacts
```

## api

### constructor

- __`constructor Client()`__

Constructs and returns a new `Client` instance

### async functions

- __`client.connect(<params:string>, callback:function(err:Error))`__

Connect to a PostgreSQL backend server. 

__params__ is _optional_ and is in any format accepted by [libpq](http://www.postgresql.org/docs/9.3/static/libpq-connect.html#LIBPQ-CONNSTRING).  The connection string is passed _as is_ to libpq, so any format supported by libpq will be supported here.  Likewise, any format _unsupported_ by libpq will not work.  If no parameters are supplied libpq will use [environment variables](http://www.postgresql.org/docs/9.3/static/libpq-envars.html) to connect.

Returns an `Error` to the `callback` if the connection was unsuccessful.  `callback` is _required_.

##### example

```js
var client = new Client()
client.connect(function(err) {
  if(err) throw err
  
  console.log('connected!')
})

var client2 = new Client()
client2.connect('postgresql://user:password@host:5432/database?param=value', function(err) {
  if(err) throw err
  
  console.log('connected with connection string!')
})
```

- __`client.query(queryText:string, <values:string[]>, callback:Function(err:Error, rows:Object[]))`__

Execute a query with the text of `queryText` and _optional_ parameters specified in the `values` array. All values are passed to the PostgreSQL backend server and executed as a parameterized statement.  The callback is _required_ and is called with an `Error` object in the event of a query error, otherwise it is passed an array of result objects.  Each element in this array is a dictionary of results with keys for column names and their values as the values for those columns.

##### example

```js
var client = new Client()
client.connect(function(err) {
  if (err) throw err
  
  client.query('SELECT NOW()', function(err, rows) {
    if (err) throw err
    
    console.log(rows) // [{ "now": "Tue Sep 16 2014 23:42:39 GMT-0400 (EDT)" }]
    
    client.query('SELECT $1::text as name', ['Brian'], function(err, rows) {
      if (err) throw err
      
      console.log(rows) // [{ "name": "Brian" }]
      
      client.end()
    })
  })
})
```


- __`client.prepare(statementName:string, queryText:string, nParams:int, callback:Function(err:Error))`__

Prepares a _named statement_ for later execution.  You _must_ supply the name of the statement via `statementName`, the command to prepare via `queryText` and the number of parameters in `queryText` via `nParams`. Calls the callback with an `Error` if there was an error.

##### example

```js
var client = new Client()
client.connect(function(err) {
  if(err) throw err
  
  client.prepare('prepared_statement', 'SELECT $1::text as name', 1, function(err) {
    if(err) throw err
    
    console.log('statement prepared')
    client.end()
  })
  
})
```

- __`client.execute(statementName:string, <values:string[]>, callback:Function(err:err, rows:Object[]))`__

Executes a previously prepared statement on this client with the name of `statementName`, passing it the optional array of query parameters as a `values` array.  The `callback` is mandatory and is called with and `Error` if the execution failed, or with the same array of results as would be passed to the callback of a `client.query` result.

##### example


```js
var client = new Client()
client.connect(function(err) {
  if(err) throw err
  
  client.prepare('i_like_beans', 'SELECT $1::text as beans', 1, function(err) {
    if(err) throw err
    
    client.execute('i_like_beans', ['Brak'], function(err, rows) {
      if(err) throw err
      
      console.log(rows) // [{ "i_like_beans": "Brak" }]
      client.end()
    })
  })
})
```

- __`client.end(<callback:Function()>`__

Ends the connection. Calls the _optional_ callback when the connection is terminated.

##### example

```js
var client = new Client()
client.connect(function(err) {
  if(err) throw err
  client.end(function() {
    console.log('client ended') // client ended
  })
})
```

- __`client.cancel(callback:function(err))`__

Cancels the active query on the client. Callback receives an error if there was an error _sending_ the cancel request.

##### example
```js
var client = new Client()
client.connectSync()
//sleep for 100 seconds
client.query('select pg_sleep(100)', function(err) {
  console.log(err) // [Error: ERROR: canceling statement due to user request]
})
client.cancel(function(err) {
  console.log('cancel dispatched')
})

```

### sync functions

- __`client.connectSync(params:string)`__

Connect to a PostgreSQL backend server. Params is in any format accepted by [libpq](http://www.postgresql.org/docs/9.3/static/libpq-connect.html#LIBPQ-CONNSTRING).  Throws an `Error` if the connection was unsuccessful.

- __`client.querySync(queryText:string, <values:string[]>) -> results:Object[]`__

Executes a query with a text of `queryText` and optional parameters as `values`. Uses a parameterized query if `values` are supplied.  Throws an `Error` if the query fails, otherwise returns an array of results.

- __`client.prepareSync(statementName:string, queryText:string, nParams:int)`__

Prepares a name statement with name of `statementName` and a query text of `queryText`. You must specify the number of params in the query with the `nParams` argument.  Throws an `Error` if the statement is un-preparable, otherwise returns an array of results.

- __`client.executeSync(statementName:string, <values:string[]>) -> results:Object[]`__

Executes a previously prepared statement on this client with the name of `statementName`, passing it the optional array of query paramters as a `values` array.  Throws an `Error` if the execution fails, otherwas returns an array of results.

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
