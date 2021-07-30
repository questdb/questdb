var PQ = module.exports = require('bindings')('addon.node').PQ;

//print out the include dir
//if you want to include this in a binding.gyp file
if(!module.parent) {
  var path = require('path');
  console.log(path.normalize(__dirname + '/src'));
}

var EventEmitter = require('events').EventEmitter;
var assert = require('assert');

for(var key in EventEmitter.prototype) {
  PQ.prototype[key] = EventEmitter.prototype[key];
}

//SYNC connects to the server
//throws an exception in the event of a connection error
PQ.prototype.connectSync = function(paramString) {
  this.connected = true;
  if(!paramString) {
    paramString = '';
  }
  var connected = this.$connectSync(paramString);
  if(!connected) {
    var err = new Error(this.errorMessage());
    this.finish();
    throw err;
  }
};

//connects async using a background thread
//calls the callback with an error if there was one
PQ.prototype.connect = function(paramString, cb) {
  this.connected = true;
  if(typeof paramString == 'function') {
    cb = paramString;
    paramString = '';
  }
  if(!paramString) {
    paramString = '';
  }
  assert(cb, 'Must provide a connection callback');
  if(process.domain) {
    cb = process.domain.bind(cb);
  }
  this.$connect(paramString, cb);
};

PQ.prototype.errorMessage = function() {
  return this.$getLastErrorMessage();
};

//returns an int for the fd of the socket
PQ.prototype.socket = function() {
  return this.$socket();
};

// return server version number e.g. 90300
PQ.prototype.serverVersion = function () {
  return this.$serverVersion();
};

//finishes the connection & closes it
PQ.prototype.finish = function() {
  this.connected = false;
  this.$finish();
};

////SYNC executes a plain text query
//immediately stores the results within the PQ object for consumption with
//ntuples, getvalue, etc...
//returns false if there was an error
//consume additional error details via PQ#errorMessage & friends
PQ.prototype.exec = function(commandText) {
  if(!commandText) {
    commandText = '';
  }
  this.$exec(commandText);
};

//SYNC executes a query with parameters
//immediately stores the results within the PQ object for consumption with
//ntuples, getvalue, etc...
//returns false if there was an error
//consume additional error details via PQ#errorMessage & friends
PQ.prototype.execParams = function(commandText, parameters) {
  if(!commandText) {
    commandText = '';
  }
  if(!parameters) {
    parameters = [];
  }
  this.$execParams(commandText, parameters);
};

//SYNC prepares a named query and stores the result
//immediately stores the results within the PQ object for consumption with
//ntuples, getvalue, etc...
//returns false if there was an error
//consume additional error details via PQ#errorMessage & friends
PQ.prototype.prepare = function(statementName, commandText, nParams) {
  assert.equal(arguments.length, 3, 'Must supply 3 arguments');
  if(!statementName) {
    statementName = '';
  }
  if(!commandText) {
    commandText = '';
  }
  nParams = Number(nParams) || 0;
  this.$prepare(statementName, commandText, nParams);
};

//SYNC executes a named, prepared query and stores the result
//immediately stores the results within the PQ object for consumption with
//ntuples, getvalue, etc...
//returns false if there was an error
//consume additional error details via PQ#errorMessage & friends
PQ.prototype.execPrepared = function(statementName, parameters) {
  if(!statementName) {
    statementName = '';
  }
  if(!parameters) {
    parameters = [];
  }
  this.$execPrepared(statementName, parameters);
};

//send a command to begin executing a query in async mode
//returns true if sent, or false if there was a send failure
PQ.prototype.sendQuery = function(commandText) {
  if(!commandText) {
    commandText = '';
  }
  return this.$sendQuery(commandText);
};

//send a command to begin executing a query with parameters in async mode
//returns true if sent, or false if there was a send failure
PQ.prototype.sendQueryParams = function(commandText, parameters) {
  if(!commandText) {
    commandText = '';
  }
  if(!parameters) {
    parameters = [];
  }
  return this.$sendQueryParams(commandText, parameters);
};

//send a command to prepare a named query in async mode
//returns true if sent, or false if there was a send failure
PQ.prototype.sendPrepare = function(statementName, commandText, nParams) {
  assert.equal(arguments.length, 3, 'Must supply 3 arguments');
  if(!statementName) {
    statementName = '';
  }
  if(!commandText) {
    commandText = '';
  }
  nParams = Number(nParams) || 0;
  return this.$sendPrepare(statementName, commandText, nParams);
};

//send a command to execute a named query in async mode
//returns true if sent, or false if there was a send failure
PQ.prototype.sendQueryPrepared = function(statementName, parameters) {
  if(!statementName) {
    statementName = '';
  }
  if(!parameters) {
    parameters = [];
  }
  return this.$sendQueryPrepared(statementName, parameters);
};

//'pops' a result out of the buffered
//response data read during async command execution
//and stores it on the c/c++ object so you can consume
//the data from it.  returns true if there was a pending result
//or false if there was no pending result. if there was no pending result
//the last found result is not overwritten so you can call getResult as many
//times as you want, and you'll always have the last available result for consumption
PQ.prototype.getResult = function() {
  return this.$getResult();
};

//returns a text of the enum associated with the result
//usually just PGRES_COMMAND_OK or PGRES_FATAL_ERROR
PQ.prototype.resultStatus = function() {
  return this.$resultStatus();
};

PQ.prototype.resultErrorMessage = function() {
  return this.$resultErrorMessage();
};

PQ.prototype.resultErrorFields = function() {
  return this.$resultErrorFields();
};

//free the memory associated with a result
//this is somewhat handled for you within the c/c++ code
//by never allowing the code to 'leak' a result. still,
//if you absolutely want to free it yourself, you can use this.
PQ.prototype.clear = function() {
  this.$clear();
};

//returns the number of tuples (rows) in the result set
PQ.prototype.ntuples = function() {
  return this.$ntuples();
};

//returns the number of fields (columns) in the result set
PQ.prototype.nfields = function() {
  return this.$nfields();
};

//returns the name of the field (column) at the given offset
PQ.prototype.fname = function(offset) {
  return this.$fname(offset);
};

//returns the Oid of the type for the given field
PQ.prototype.ftype = function(offset) {
  return this.$ftype(offset);
};

//returns a text value at the given row/col
//if the value is null this still returns empty string
//so you need to use PQ#getisnull to determine
PQ.prototype.getvalue = function(row, col) {
  return this.$getvalue(row, col);
};

//returns true/false if the value is null
PQ.prototype.getisnull = function(row, col) {
  return this.$getisnull(row, col);
};

//returns the status of the command
PQ.prototype.cmdStatus = function() {
  return this.$cmdStatus();
};

//returns the tuples in the command
PQ.prototype.cmdTuples = function() {
  return this.$cmdTuples();
};

//starts the 'read ready' libuv socket listener.
//Once the socket becomes readable, the PQ instance starts
//emitting 'readable' events.  Similar to how node's readable-stream
//works except to clear the SELECT() notification you need to call
//PQ#consumeInput instead of letting node pull the data off the socket
//http://www.postgresql.org/docs/9.1/static/libpq-async.html
PQ.prototype.startReader = function() {
  assert(this.connected, 'Must be connected to start reader');
  this.$startRead();
};

//suspends the libuv socket 'read ready' listener
PQ.prototype.stopReader = function() {
  this.$stopRead();
};

PQ.prototype.writable = function(cb) {
  assert(this.connected, 'Must be connected to start writer');
  this.$startWrite();
  return this.once('writable', cb);
};

//returns boolean - false indicates an error condition
//e.g. a failure to consume input
PQ.prototype.consumeInput = function() {
  return this.$consumeInput();
};

//returns true if PQ#getResult would cause
//the process to block waiting on results
//false indicates PQ#getResult can be called
//with an assurance of not blocking
PQ.prototype.isBusy = function() {
  return this.$isBusy();
};

//toggles the socket blocking on outgoing writes
PQ.prototype.setNonBlocking = function(truthy) {
  return this.$setNonBlocking(truthy ? 1 : 0);
};

//returns true if the connection is non-blocking on writes, otherwise false
//note: connection is always non-blocking on reads if using the send* methods
PQ.prototype.isNonBlocking = function() {
  return this.$isNonBlocking();
};

//returns 1 if socket is not write-ready
//returns 0 if all data flushed to socket
//returns -1 if there is an error
PQ.prototype.flush = function() {
  return this.$flush();
};

//escapes a literal and returns the escaped string
//I'm not 100% sure this doesn't do any I/O...need to check that
PQ.prototype.escapeLiteral = function(input) {
  if(!input) return input;
  return this.$escapeLiteral(input);
};

PQ.prototype.escapeIdentifier = function(input) {
  if(!input) return input;
  return this.$escapeIdentifier(input);
};

//Checks for any notifications which may have arrivied
//and returns them as a javascript object: {relname: 'string', extra: 'string', be_pid: int}
//if there are no pending notifications this returns undefined
PQ.prototype.notifies = function() {
  return this.$notifies();
};

//Sends a buffer of binary data to the server
//returns 1 if the command was sent successfully
//returns 0 if the command would block (use PQ#writable here if so)
//returns -1 if there was an error
PQ.prototype.putCopyData = function(buffer) {
  assert(buffer instanceof Buffer);
  return this.$putCopyData(buffer);
};

//Sends a command to 'finish' the copy
//if an error message is passed, it will be sent to the
//backend and signal a request to cancel the copy in
//returns 1 if sent succesfully
//returns 0 if the command would block
//returns -1 if there was an error
PQ.prototype.putCopyEnd = function(errorMessage) {
  if(errorMessage) {
    return this.$putCopyEnd(errorMessage);
  }
  return this.$putCopyEnd();
};

//Gets a buffer of data from a copy out command
//if async is passed as true it will not block waiting
//for the result, otherwise this will BLOCK for a result.
//returns a buffer if successful
//returns 0 if copy is still in process (async only)
//returns -1 if the copy is done
//returns -2 if there was an error
PQ.prototype.getCopyData = function(async) {
  return this.$getCopyData(!!async);
};

PQ.prototype.cancel = function() {
  return this.$cancel();
};
