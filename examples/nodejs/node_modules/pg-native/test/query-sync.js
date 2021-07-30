var Client = require('../')
var assert = require('assert')

describe('query sync', function (done) {
  before(function () {
    this.client = Client()
    this.client.connectSync()
  })

  after(function (done) {
    this.client.end(done)
  })

  it('simple query works', function () {
    var rows = this.client.querySync('SELECT NOW() AS the_time')
    assert.equal(rows.length, 1)
    assert.equal(rows[0].the_time.getFullYear(), new Date().getFullYear())
  })

  it('parameterized query works', function () {
    var rows = this.client.querySync('SELECT $1::text AS name', ['Brian'])
    assert.equal(rows.length, 1)
    assert.equal(rows[0].name, 'Brian')
  })

  it('prepared statement works', function () {
    this.client.prepareSync('test', 'SELECT $1::text as name', 1)

    var rows = this.client.executeSync('test', ['Brian'])
    assert.equal(rows.length, 1)
    assert.equal(rows[0].name, 'Brian')

    var rows2 = this.client.executeSync('test', ['Aaron'])
    assert.equal(rows2.length, 1)
    assert.equal(rows2[0].name, 'Aaron')
  })

  it('prepare throws exception on error', function () {
    assert.throws(function () {
      this.client.prepareSync('blah', 'I LIKE TO PARTY!!!', 0)
    }.bind(this))
  })

  it('throws exception on executing improperly', function () {
    assert.throws(function () {
      // wrong number of parameters
      this.client.executeSync('test', [])
    })
  })

  it('throws exception on error', function () {
    assert.throws(function () {
      this.client.querySync('SELECT ASLKJASLKJF')
    }.bind(this))
  })

  it('is still usable after an error', function () {
    var rows = this.client.querySync('SELECT NOW()')
    assert(rows, 'should have returned rows')
    assert.equal(rows.length, 1)
  })

  it('supports empty query', function () {
    var rows = this.client.querySync('')
    assert(rows, 'should return rows')
    assert.equal(rows.length, 0, 'should return no rows')
  })
})
