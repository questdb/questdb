var Libpq = require('../');
var assert = require('assert');

describe('Retrieve server version from connection', function() {

  it('return version number when connected', function() {
    var pq = new Libpq();
    pq.connectSync();
    var version = pq.serverVersion();
    assert.equal(typeof version, 'number');
    assert(version > 60000);
  });

  it('return zero when not connected', function() {
    var pq = new Libpq();
    var version = pq.serverVersion();
    assert.equal(typeof version, 'number');
    assert.equal(version, 0);
  });

});