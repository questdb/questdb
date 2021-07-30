var Client = require('../')
var assert = require('assert')

describe('cancel query', function () {
  it('works', function (done) {
    var client = new Client()
    client.connectSync()
    client.query('SELECT pg_sleep(100);', function (err) {
      assert(err instanceof Error)
      client.end(done)
    })
    client.cancel(function (err) {
      assert.ifError(err)
    })
  })

  it('does not raise error if no active query', function (done) {
    var client = new Client()
    client.connectSync()
    client.cancel(function (err) {
      assert.ifError(err)
      done()
    })
  })

  it('raises error if client is not connected', function (done) {
    new Client().cancel(function (err) {
      assert(err, 'should raise an error when not connected')
      done()
    })
  })
})
