var assert = require('assert');
var helper = require('./helper');

describe('result accessors', function() {
  helper.setupIntegration();

  before(function() {
    this.pq.exec("INSERT INTO test_data(name, age) VALUES ('bob', 80) RETURNING *");
    assert(!this.pq.errorMessage());
  });

  it('has ntuples', function() {
    assert.strictEqual(this.pq.ntuples(), 1);
  });

  it('has cmdStatus', function() {
    assert.equal(this.pq.cmdStatus(), 'INSERT 0 1');
  });

  it('has command tuples', function() {
    assert.strictEqual(this.pq.cmdTuples(), '1');
  });
});
