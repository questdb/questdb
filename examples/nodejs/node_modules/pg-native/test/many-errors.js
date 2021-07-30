var Client = require('../')
var async = require('async')
var assert = require('assert')

describe('many errors', function () {
  it('functions properly without segfault', function (done) {
    var throwError = function (n, cb) {
      var client = new Client()
      client.connectSync()

      var doIt = function (n, cb) {
        client.query('select asdfiasdf', function (err) {
          assert(err, 'bad query should emit an error')
          cb(null)
        })
      }

      async.timesSeries(10, doIt, function (err) {
        if (err) return cb(err)
        client.end(cb)
      })
    }

    async.times(10, throwError, done)
  })
})
