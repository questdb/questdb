var Client = require('../')
var ok = require('okay')

var notify = function (channel, payload) {
  var client = new Client()
  client.connectSync()
  client.querySync('NOTIFY ' + channel + ", '" + payload + "'")
  client.end()
}

describe('simple LISTEN/NOTIFY', function () {
  before(function (done) {
    var client = this.client = new Client()
    client.connect(done)
  })

  it('works', function (done) {
    var client = this.client
    client.querySync('LISTEN boom')
    client.on('notification', function (msg) {
      done()
    })
    notify('boom', 'sup')
  })

  after(function (done) {
    this.client.end(done)
  })
})

if (!process.env.TRAVIS_CI) {
  describe('async LISTEN/NOTIFY', function () {
    before(function (done) {
      var client = this.client = new Client()
      client.connect(done)
    })

    it('works', function (done) {
      var client = this.client
      var count = 0
      var check = function () {
        count++
        if (count >= 2) return done()
      }
      client.on('notification', check)
      client.query('LISTEN test', ok(done, function () {
        notify('test', 'bot')
        client.query('SELECT pg_sleep(.05)', ok(done, function () {
          check()
        }))
        notify('test', 'bot')
      }))
    })

    after(function (done) {
      this.client.end(done)
    })
  })
}
