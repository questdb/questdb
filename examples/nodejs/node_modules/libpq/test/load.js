var Libpq = require('../')
var async = require('async')
var ok = require('okay')

var blink = function(n, cb) {
  var connections = []
  for(var i = 0; i < 30; i++) {
    connections.push(new Libpq())
  }
  var connect = function(con, cb) {
    con.connect(cb)
  }
  async.each(connections, connect, ok(function() {
    connections.forEach(function(con) {
      con.finish()
    })
    cb()
  }))
}

describe('many connections', function() {
  it('works', function(done) {
    async.timesSeries(10, blink, done)
  })
})
