var helper = require('./helper');
var assert = require('assert');
var bufferFrom = require('buffer-from')

describe('COPY IN', function() {
  helper.setupIntegration();

  it('check existing data assuptions', function() {
    this.pq.exec('SELECT COUNT(*) FROM test_data');
    assert.equal(this.pq.getvalue(0, 0), 3);
  });

  it('copies data in', function() {
    var success = this.pq.exec('COPY test_data FROM stdin');
    assert.equal(this.pq.resultStatus(), 'PGRES_COPY_IN');

    var buffer = bufferFrom("bob\t100\n", 'utf8');
    var res = this.pq.putCopyData(buffer);
    assert.strictEqual(res, 1);

    var res = this.pq.putCopyEnd();
    assert.strictEqual(res, 1);

    while(this.pq.getResult()) {}

    this.pq.exec('SELECT COUNT(*) FROM test_data');
    assert.equal(this.pq.getvalue(0, 0), 4);
  });

  it('can cancel copy data in', function() {
    var success = this.pq.exec('COPY test_data FROM stdin');
    assert.equal(this.pq.resultStatus(), 'PGRES_COPY_IN');

    var buffer = bufferFrom("bob\t100\n", 'utf8');
    var res = this.pq.putCopyData(buffer);
    assert.strictEqual(res, 1);

    var res = this.pq.putCopyEnd('cancel!');
    assert.strictEqual(res, 1);

    while(this.pq.getResult()) {}
    assert(this.pq.errorMessage());
    assert(this.pq.errorMessage().indexOf('cancel!') > -1, this.pq.errorMessage() + ' should have contained "cancel!"');

    this.pq.exec('SELECT COUNT(*) FROM test_data');
    assert.equal(this.pq.getvalue(0, 0), 4);
  });
});
