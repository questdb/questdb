var PQ = require('../')
var assert = require('assert');

describe('async connection', function() {
  it('works', function(done) {
    var pq = new PQ();
    assert(!pq.connected, 'should have connected set to falsy');
    pq.connect(function(err) {
      assert(!err);
      pq.exec('SELECT NOW()');
      assert.equal(pq.connected, true, 'should have connected set to true');
      assert.equal(pq.ntuples(), 1);
      done();
    });
  });

  it('works with hard-coded connection parameters', function(done) {
    var pq = new PQ();
    var conString ='host=' + (process.env.PGHOST || 'localhost');
    pq.connect(conString, done);
  });

  it('returns an error to the callback if connection fails', function(done) {
    new PQ().connect('host=asldkfjasldkfjalskdfjasdf', function(err) {
      assert(err, 'should have passed an error');
      done();
    });
  });

  it('respects the active domain', function(done) {
    var pq = new PQ();
    var domain = require('domain').create();
    domain.run(function() {
      var activeDomain = process.domain;
      assert(activeDomain, 'Should have an active domain');
      pq.connect(function(err) {
        assert.strictEqual(process.domain, activeDomain, 'Active domain is lost');
        done();
      });
    });
  });
});
