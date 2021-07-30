var Libpq = require('libpq')
var EventEmitter = require('events').EventEmitter
var util = require('util')
var assert = require('assert')
var types = require('pg-types')
var buildResult = require('./lib/build-result')
var CopyStream = require('./lib/copy-stream')

var Client = module.exports = function (config) {
  if (!(this instanceof Client)) {
    return new Client(config)
  }

  config = config || {}

  EventEmitter.call(this)
  this.pq = new Libpq()
  this._reading = false
  this._read = this._read.bind(this)

  // allow custom type converstion to be passed in
  this._types = config.types || types

  // allow config to specify returning results
  // as an array of values instead of a hash
  this.arrayMode = config.arrayMode || false
  this._resultCount = 0
  this._rows = undefined
  this._results = undefined

  // lazy start the reader if notifications are listened for
  // this way if you only run sync queries you wont block
  // the event loop artificially
  this.on('newListener', (event) => {
    if (event !== 'notification') return
    this._startReading()
  })

  this.on('result', this._onResult.bind(this))
  this.on('readyForQuery', this._onReadyForQuery.bind(this))
}

util.inherits(Client, EventEmitter)

Client.prototype.connect = function (params, cb) {
  this.pq.connect(params, cb)
}

Client.prototype.connectSync = function (params) {
  this.pq.connectSync(params)
}

Client.prototype.query = function (text, values, cb) {
  var queryFn

  if (typeof values === 'function') {
    cb = values
    queryFn = function () { return self.pq.sendQuery(text) }
  } else {
    queryFn = function () { return self.pq.sendQueryParams(text, values) }
  }

  var self = this

  self._dispatchQuery(self.pq, queryFn, function (err) {
    if (err) return cb(err)

    self._awaitResult(cb)
  })
}

Client.prototype.prepare = function (statementName, text, nParams, cb) {
  var self = this
  var fn = function () {
    return self.pq.sendPrepare(statementName, text, nParams)
  }

  self._dispatchQuery(self.pq, fn, function (err) {
    if (err) return cb(err)
    self._awaitResult(cb)
  })
}

Client.prototype.execute = function (statementName, parameters, cb) {
  var self = this

  var fn = function () {
    return self.pq.sendQueryPrepared(statementName, parameters)
  }

  self._dispatchQuery(self.pq, fn, function (err, rows) {
    if (err) return cb(err)
    self._awaitResult(cb)
  })
}

Client.prototype.getCopyStream = function () {
  this.pq.setNonBlocking(true)
  this._stopReading()
  return new CopyStream(this.pq)
}

// cancel a currently executing query
Client.prototype.cancel = function (cb) {
  assert(cb, 'Callback is required')
  // result is either true or a string containing an error
  var result = this.pq.cancel()
  return setImmediate(function () {
    cb(result === true ? undefined : new Error(result))
  })
}

Client.prototype.querySync = function (text, values) {
  if (values) {
    this.pq.execParams(text, values)
  } else {
    this.pq.exec(text)
  }

  throwIfError(this.pq)
  const result = buildResult(this.pq, this._types, this.arrayMode)
  return result.rows
}

Client.prototype.prepareSync = function (statementName, text, nParams) {
  this.pq.prepare(statementName, text, nParams)
  throwIfError(this.pq)
}

Client.prototype.executeSync = function (statementName, parameters) {
  this.pq.execPrepared(statementName, parameters)
  throwIfError(this.pq)
  return buildResult(this.pq, this._types, this.arrayMode).rows
}

Client.prototype.escapeLiteral = function (value) {
  return this.pq.escapeLiteral(value)
}

Client.prototype.escapeIdentifier = function (value) {
  return this.pq.escapeIdentifier(value)
}

// export the version number so we can check it in node-postgres
module.exports.version = require('./package.json').version

Client.prototype.end = function (cb) {
  this._stopReading()
  this.pq.finish()
  if (cb) setImmediate(cb)
}

Client.prototype._readError = function (message) {
  var err = new Error(message || this.pq.errorMessage())
  this.emit('error', err)
}

Client.prototype._stopReading = function () {
  if (!this._reading) return
  this._reading = false
  this.pq.stopReader()
  this.pq.removeListener('readable', this._read)
}

Client.prototype._consumeQueryResults = function (pq) {
  return buildResult(pq, this._types, this.arrayMode)
}

Client.prototype._emitResult = function (pq) {
  var status = pq.resultStatus()
  switch (status) {
    case 'PGRES_FATAL_ERROR':
      this._queryError = new Error(this.pq.resultErrorMessage())
      break

    case 'PGRES_TUPLES_OK':
    case 'PGRES_COMMAND_OK':
    case 'PGRES_EMPTY_QUERY':
      const result = this._consumeQueryResults(this.pq)
      this.emit('result', result)
      break

    case 'PGRES_COPY_OUT':
    case 'PGRES_COPY_BOTH': {
      break
    }

    default:
      this._readError('unrecognized command status: ' + status)
      break
  }
  return status
}

// called when libpq is readable
Client.prototype._read = function () {
  var pq = this.pq
  // read waiting data from the socket
  // e.g. clear the pending 'select'
  if (!pq.consumeInput()) {
    // if consumeInput returns false
    // than a read error has been encountered
    return this._readError()
  }

  // check if there is still outstanding data
  // if so, wait for it all to come in
  if (pq.isBusy()) {
    return
  }

  // load our result object

  while (pq.getResult()) {
    const resultStatus = this._emitResult(this.pq)

    // if the command initiated copy mode we need to break out of the read loop
    // so a substream can begin to read copy data
    if (resultStatus === 'PGRES_COPY_BOTH' || resultStatus === 'PGRES_COPY_OUT') {
      break
    }

    // if reading multiple results, sometimes the following results might cause
    // a blocking read. in this scenario yield back off the reader until libpq is readable
    if (pq.isBusy()) {
      return
    }
  }

  this.emit('readyForQuery')

  var notice = this.pq.notifies()
  while (notice) {
    this.emit('notification', notice)
    notice = this.pq.notifies()
  }
}

// ensures the client is reading and
// everything is set up for async io
Client.prototype._startReading = function () {
  if (this._reading) return
  this._reading = true
  this.pq.on('readable', this._read)
  this.pq.startReader()
}

var throwIfError = function (pq) {
  var err = pq.resultErrorMessage() || pq.errorMessage()
  if (err) {
    throw new Error(err)
  }
}

Client.prototype._awaitResult = function (cb) {
  this._queryCallback = cb
  return this._startReading()
}

// wait for the writable socket to drain
Client.prototype._waitForDrain = function (pq, cb) {
  var res = pq.flush()
  // res of 0 is success
  if (res === 0) return cb()

  // res of -1 is failure
  if (res === -1) return cb(pq.errorMessage())

  // otherwise outgoing message didn't flush to socket
  // wait for it to flush and try again
  var self = this
  // you cannot read & write on a socket at the same time
  return pq.writable(function () {
    self._waitForDrain(pq, cb)
  })
}

// send an async query to libpq and wait for it to
// finish writing query text to the socket
Client.prototype._dispatchQuery = function (pq, fn, cb) {
  this._stopReading()
  var success = pq.setNonBlocking(true)
  if (!success) return cb(new Error('Unable to set non-blocking to true'))
  var sent = fn()
  if (!sent) return cb(new Error(pq.errorMessage() || 'Something went wrong dispatching the query'))
  this._waitForDrain(pq, cb)
}

Client.prototype._onResult = function (result) {
  if (this._resultCount === 0) {
    this._results = result
    this._rows = result.rows
  } else if (this._resultCount === 1) {
    this._results = [this._results, result]
    this._rows = [this._rows, result.rows]
  } else {
    this._results.push(result)
    this._rows.push(result.rows)
  }
  this._resultCount++
}

Client.prototype._onReadyForQuery = function () {
  // remove instance callback
  const cb = this._queryCallback
  this._queryCallback = undefined

  // remove instance query error
  const err = this._queryError
  this._queryError = undefined

  // remove instance rows
  const rows = this._rows
  this._rows = undefined

  // remove instance results
  const results = this._results
  this._results = undefined

  this._resultCount = 0

  if (cb) {
    cb(err, rows || [], results)
  }
}
