var helper = require('./helper');
var assert = require('assert');

describe('COPY OUT', function() {
  helper.setupIntegration();

  var getRow = function(pq, expected) {
    var result = pq.getCopyData(false);
    assert(result instanceof Buffer, 'Result should be a buffer');
    assert.equal(result.toString('utf8'), expected);
  };

  it('copies data out', function() {
    this.pq.exec('COPY test_data TO stdin');
    assert.equal(this.pq.resultStatus(), 'PGRES_COPY_OUT');
    getRow(this.pq, 'brian\t32\n');
    getRow(this.pq, 'aaron\t30\n');
    getRow(this.pq, '\t\\N\n');
    assert.strictEqual(this.pq.getCopyData(), -1);
  });
});
