var helper = require('./helper')
var assert = require('assert')

describe('set & get non blocking', function() {
  helper.setupIntegration();
  it('is initially set to false', function() {
    assert.strictEqual(this.pq.isNonBlocking(), false);
  });

  it('can switch back and forth', function() {
    assert.strictEqual(this.pq.setNonBlocking(true), true);
    assert.strictEqual(this.pq.isNonBlocking(), true);
    assert.strictEqual(this.pq.setNonBlocking(), true);
    assert.strictEqual(this.pq.isNonBlocking(), false);
  });
});
