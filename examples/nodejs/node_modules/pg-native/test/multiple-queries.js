var Client = require('../')
var assert = require('assert')

describe('multiple commands in a single query', function () {
  before(function (done) {
    this.client = new Client()
    this.client.connect(done)
  })

  after(function (done) {
    this.client.end(done)
  })

  it('all execute to completion', function (done) {
    this.client.query("SELECT '10'::int as num; SELECT 'brian'::text as name", function (err, rows) {
      assert.ifError(err)
      assert.equal(rows.length, 2, 'should return two sets rows')
      assert.equal(rows[0][0].num, '10')
      assert.equal(rows[1][0].name, 'brian')
      done()
    })
  })

  it('inserts and reads at once', function (done) {
    var txt = 'CREATE TEMP TABLE boom(age int);'
    txt += 'INSERT INTO boom(age) VALUES(10);'
    txt += 'SELECT * FROM boom;'
    this.client.query(txt, function (err, rows, results) {
      assert.ifError(err)
      assert.equal(rows.length, 3)
      assert.equal(rows[0].length, 0)
      assert.equal(rows[1].length, 0)
      assert.equal(rows[2][0].age, 10)

      assert.equal(results[0].command, 'CREATE')
      assert.equal(results[1].command, 'INSERT')
      assert.equal(results[2].command, 'SELECT')
      done()
    })
  })
})
