var Client = require('../')
var async = require('async')

var loop = function () {
  var client = new Client()

  var connect = function (cb) {
    client.connect(cb)
  }

  var simpleQuery = function (cb) {
    client.query('SELECT NOW()', cb)
  }

  var paramsQuery = function (cb) {
    client.query('SELECT $1::text as name', ['Brian'], cb)
  }

  var prepared = function (cb) {
    client.prepare('test', 'SELECT $1::text as name', 1, function (err) {
      if (err) return cb(err)
      client.execute('test', ['Brian'], cb)
    })
  }

  var sync = function (cb) {
    client.querySync('SELECT NOW()')
    client.querySync('SELECT $1::text as name', ['Brian'])
    client.prepareSync('boom', 'SELECT $1::text as name', 1)
    client.executeSync('boom', ['Brian'])
    setImmediate(cb)
  }

  var end = function (cb) {
    client.end(cb)
  }

  var ops = [
    connect,
    simpleQuery,
    paramsQuery,
    prepared,
    sync,
    end
  ]

  var start = Date.now()
  async.series(ops, function (err) {
    if (err) throw err
    console.log(Date.now() - start)
    setImmediate(loop)
  })
}

// on my machine this will consume memory up to about 50 megs of ram
// and then stabalize at that point
loop()
