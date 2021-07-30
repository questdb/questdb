'use strict'

var Client = require('../')
var assert = require('assert')

describe('connection errors', function () {
  it('raise error events', function (done) {
    var client = new Client()
    client.connectSync()
    client.query('SELECT pg_terminate_backend(pg_backend_pid())', assert.fail)
    client.on('error', function (err) {
      assert(/^server closed the connection unexpectedly/.test(err.message))
      assert.strictEqual(client.pq.resultErrorFields().sqlState, '57P01')
      client.end()
      done()
    })
  })
})
