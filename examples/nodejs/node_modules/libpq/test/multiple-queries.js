var Libpq = require('../');
var ok = require('okay')

var queryText = "SELECT * FROM generate_series(1, 1000)"

var query = function(pq, cb) {
  var sent = pq.sendQuery(queryText);
  if(!sent) return cb(new Error(pg.errorMessage()));
  console.log('sent query')

  //consume any outstanding results
  //while(!pq.isBusy() && pq.getResult()) {
    //console.log('consumed unused result')
  //}

  var cleanup = function() {
    pq.removeListener('readable', onReadable);
    pq.stopReader();
  }

  var readError = function(message) {
    cleanup();
    return cb(new Error(message || pq.errorMessage));
  };

  var onReadable = function() {
    //read waiting data from the socket
    //e.g. clear the pending 'select'
    if(!pq.consumeInput()) {
      return readError();
    }
    //check if there is still outstanding data
    //if so, wait for it all to come in
    if(pq.isBusy()) {
      return;
    }
    //load our result object
    pq.getResult();

    //"read until results return null"
    //or in our case ensure we only have one result
    if(pq.getResult()) {
      return readError('Only one result at a time is accepted');
    }
    cleanup();
    return cb(null, [])
  };
  pq.on('readable', onReadable);
  pq.startReader();
};

describe('multiple queries', function() {
  var pq = new Libpq();

  before(function(done) {
    pq.connect(done)
  })

  it('first query works', function(done) {
    query(pq, done);
  });

  it('second query works', function(done) {
    query(pq, done);
  });

  it('third query works', function(done) {
    query(pq, done);
  });
});
