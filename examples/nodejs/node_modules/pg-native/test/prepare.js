var Client = require('../')
var ok = require('okay')
var async = require('async')

describe('async prepare', function () {
  var run = function (n, cb) {
    var client = new Client()
    client.connectSync()

    var exec = function (x, done) {
      client.prepare('get_now' + x, 'SELECT NOW()', 0, done)
    }

    async.timesSeries(10, exec, ok(cb, function () {
      client.end(cb)
    }))
  }

  var t = function (n) {
    it('works for ' + n + ' clients', function (done) {
      async.times(n, run, function (err) {
        done(err)
      })
    })
  }

  for (var i = 0; i < 10; i++) {
    t(i)
  }
})

describe('async execute', function () {
  var run = function (n, cb) {
    var client = new Client()
    client.connectSync()
    client.prepareSync('get_now', 'SELECT NOW()', 0)
    var exec = function (x, cb) {
      client.execute('get_now', [], cb)
    }
    async.timesSeries(10, exec, ok(cb, function () {
      client.end(cb)
    }))
  }

  var t = function (n) {
    it('works for ' + n + ' clients', function (done) {
      async.times(n, run, function (err) {
        done(err)
      })
    })
  }

  for (var i = 0; i < 10; i++) {
    t(i)
  }
})
