var LibPQ = require('../')
var helper = require('./helper')
var assert = require('assert');

var consume = function(pq, cb) {
  if(!pq.isBusy()) return cb();
  pq.startReader();
  var onReadable = function() {
    assert(pq.consumeInput(), pq.errorMessage());
    if(pq.isBusy()) {
      console.log('consuming a 2nd buffer of input later...')
      return;
    }
    pq.removeListener('readable', onReadable);
    pq.stopReader();
    cb();
  }
  pq.on('readable', onReadable);
}

describe('async simple query', function() {
  helper.setupIntegration();

  it('dispatches simple query', function(done) {
    var pq = this.pq;
    assert(this.pq.setNonBlocking(true));
    this.pq.writable(function() {
      var success = pq.sendQuery('SELECT 1');
      assert.strictEqual(pq.flush(), 0, 'Should have flushed all data to socket');
      assert(success, pq.errorMessage());
      consume(pq, function() {
        assert(!pq.errorMessage());
        assert(pq.getResult());
        assert.strictEqual(pq.getResult(), false);
        assert.strictEqual(pq.ntuples(), 1);
        assert.strictEqual(pq.getvalue(0, 0), '1');
        done();
      });
    });
  });

  it('dispatches parameterized query', function(done) {
    var pq = this.pq;
    var success = pq.sendQueryParams('SELECT $1::text as name', ['Brian']);
    assert(success, pq.errorMessage());
    assert.strictEqual(pq.flush(), 0, 'Should have flushed query text & parameters');
    consume(pq, function() {
      assert(!pq.errorMessage());
      assert(pq.getResult());
      assert.strictEqual(pq.getResult(), false);
      assert.strictEqual(pq.ntuples(), 1);
      assert.equal(pq.getvalue(0, 0), 'Brian');
      done();
    })
  });

  it('dispatches named query', function(done) {
    var pq = this.pq;
    var statementName = 'async-get-name';
    var success = pq.sendPrepare(statementName, 'SELECT $1::text as name', 1);
    assert(success, pq.errorMessage());
    assert.strictEqual(pq.flush(), 0, 'Should have flushed query text');
    consume(pq, function() {
      assert(!pq.errorMessage());

      //first time there should be a result
      assert(pq.getResult());

      //call 'getResult' until it returns false indicating
      //there is no more input to consume
      assert.strictEqual(pq.getResult(), false);

      //since we only prepared a statement there should be
      //0 tuples in the result
      assert.equal(pq.ntuples(), 0);

      //now execute the previously prepared statement
      var success = pq.sendQueryPrepared(statementName, ['Brian']);
      assert(success, pq.errorMessage());
      assert.strictEqual(pq.flush(), 0, 'Should have flushed parameters');
      consume(pq, function() {
        assert(!pq.errorMessage());

        //consume the result of the query execution
        assert(pq.getResult());
        assert.equal(pq.ntuples(), 1);
        assert.equal(pq.getvalue(0, 0), 'Brian');

        //call 'getResult' again to ensure we're finished
        assert.strictEqual(pq.getResult(), false);
        done();
      });
    });
  });
});
