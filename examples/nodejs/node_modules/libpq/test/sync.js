var PQ = require('../')
var assert = require('assert');

describe('connecting with bad credentials', function() {
  it('throws an error', function() {
    try {
      new PQ().connectSync('asldkfjlasdf');
    } catch(e) {
      assert.equal(e.toString().indexOf('connection pointer is NULL'), -1)
      return;
    }
    assert.fail('Should have thrown an exception');
  });
});

describe('connecting with no credentials', function() {
  before(function() {
    this.pq = new PQ();
    this.pq.connectSync();
  });

  it('is connected', function() {
    assert(this.pq.connected, 'should have connected == true');
  });

  after(function() {
    this.pq.finish();
    assert(!this.pq.connected);
  });
});

describe('result checking', function() {
  before(function() {
    this.pq = new PQ();
    this.pq.connectSync();
  });

  after(function() {
    this.pq.finish();
  });

  it('executes query', function() {
    this.pq.exec('SELECT NOW() as my_col');
    assert.equal(this.pq.resultStatus(), 'PGRES_TUPLES_OK');
  })

  it('has 1 tuple', function() {
    assert.equal(this.pq.ntuples(), 1);
  });

  it('has 1 field', function() {
    assert.strictEqual(this.pq.nfields(), 1);
  });

  it('has column name', function() {
    assert.equal(this.pq.fname(0), 'my_col');
  });

  it('has oid type of timestamptz', function() {
    assert.strictEqual(this.pq.ftype(0), 1184);
  });

  it('has value as a date', function() {
    var now = new Date();
    var val = this.pq.getvalue(0);
    var date = new Date(Date.parse(val));
    assert.equal(date.getFullYear(), now.getFullYear());
    assert.equal(date.getMonth(), now.getMonth());
  });

  it('can manually clear result multiple times', function() {
    this.pq.clear();
    this.pq.clear();
    this.pq.clear();
  });
});
