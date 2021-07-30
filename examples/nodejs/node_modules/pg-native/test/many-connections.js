var Client = require('../')
var async = require('async')
var ok = require('okay')
var bytes = require('crypto').pseudoRandomBytes

describe('many connections', function () {
  describe('async', function () {
    var test = function (count, times) {
      it('connecting ' + count + ' clients ' + times, function (done) {
        this.timeout(200000)

        var connectClient = function (n, cb) {
          var client = new Client()
          client.connect(ok(cb, function () {
            bytes(1000, ok(cb, function (chunk) {
              client.query('SELECT $1::text as txt', [chunk.toString('base64')], ok(cb, function (rows) {
                client.end(cb)
              }))
            }))
          }))
        }

        var run = function (n, cb) {
          async.times(count, connectClient, cb)
        }

        async.timesSeries(times, run, done)
      })
    }

    test(1, 1)
    test(1, 1)
    test(1, 1)
    test(5, 5)
    test(5, 5)
    test(5, 5)
    test(5, 5)
    test(10, 10)
    test(10, 10)
    test(10, 10)
    test(20, 20)
    test(20, 20)
    test(20, 20)
    test(30, 10)
    test(30, 10)
    test(30, 10)
  })
})
