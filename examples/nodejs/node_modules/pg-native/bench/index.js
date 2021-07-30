var pg = require('pg').native
var Native = require('../')

var warmup = function (fn, cb) {
  var count = 0
  var max = 10
  var run = function (err) {
    if (err) return cb(err)

    if (max >= count++) {
      return fn(run)
    }

    cb()
  }
  run()
}

var native = Native()
native.connectSync()

var queryText = 'SELECT generate_series(0, 1000)'
var client = new pg.Client()
client.connect(function () {
  var pure = function (cb) {
    client.query(queryText, function (err) {
      if (err) throw err
      cb(err)
    })
  }
  var nativeQuery = function (cb) {
    native.query(queryText, function (err) {
      if (err) throw err
      cb(err)
    })
  }

  var run = function () {
    var start = Date.now()
    warmup(pure, function () {
      console.log('pure done', Date.now() - start)
      start = Date.now()
      warmup(nativeQuery, function () {
        console.log('native done', Date.now() - start)
      })
    })
  }

  setInterval(function () {
    run()
  }, 500)
})
