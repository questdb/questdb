var PQ = require('../')
var assert = require('assert');
var helper = require('./helper')

describe('low-level query integration tests', function() {

  helper.setupIntegration();

  describe('exec', function() {
    before(function() {
      this.pq.exec('SELECT * FROM test_data');
    });

    it('has correct tuples', function() {
      assert.strictEqual(this.pq.ntuples(), 3);
    });

    it('has correct field count', function() {
      assert.strictEqual(this.pq.nfields(), 2);
    });

    it('has correct rows', function() {
      assert.strictEqual(this.pq.getvalue(0, 0), 'brian');
      assert.strictEqual(this.pq.getvalue(1, 1), '30');
      assert.strictEqual(this.pq.getvalue(2, 0), '');
      assert.strictEqual(this.pq.getisnull(2, 0), false);
      assert.strictEqual(this.pq.getvalue(2, 1), '');
      assert.strictEqual(this.pq.getisnull(2, 1), true);
    });
  });
});
