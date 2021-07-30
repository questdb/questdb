var LibPQ = require('../')
var helper = require('./helper')
var assert = require('assert');

describe('getting socket', function() {
  helper.setupIntegration();

  it('returns -1 when not connected', function() {
    var pq = new LibPQ();
    assert.equal(pq.socket(), -1);
  });

  it('returns value when connected', function() {
    assert(this.pq.socket() > 0);
  });
});
