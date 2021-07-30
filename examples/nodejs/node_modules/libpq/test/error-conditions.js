var PQ = require('../')
var assert = require('assert');

describe('without being connected', function() {
  it('exec fails', function() {
    var pq = new PQ();
    pq.exec();
    assert.equal(pq.resultStatus(), 'PGRES_FATAL_ERROR');
    assert(pq.errorMessage());
  });

  it('fails on async query', function() {
    var pq = new PQ();
    var success = pq.sendQuery('blah');
    assert.strictEqual(success, false);
    assert.equal(pq.resultStatus(), 'PGRES_FATAL_ERROR');
    assert(pq.errorMessage());
  });

  it('throws when reading while not connected', function() {
    var pq = new PQ();
    assert.throws(function() {
      pq.startReader();
    });
  });

  it('throws when writing while not connected', function() {
    var pq = new PQ();
    assert.throws(function() {
      pq.writable(function() {
      });
    });
  });
})
