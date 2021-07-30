var Client = require('../')
var assert = require('assert')

describe('connection', function () {
  it('works', function (done) {
    Client().connect(done)
  })

  it('connects with args', function (done) {
    Client().connect('host=localhost', done)
  })

  it('errors out with bad connection args', function (done) {
    Client().connect('host=asldkfjasdf', function (err) {
      assert(err, 'should raise an error for bad host')
      done()
    })
  })
})

describe('connectSync', function () {
  it('works without args', function () {
    Client().connectSync()
  })

  it('works with args', function () {
    var args = 'host=' + (process.env.PGHOST || 'localhost')
    Client().connectSync(args)
  })

  it('throws if bad host', function () {
    assert.throws(function () {
      Client().connectSync('host=laksdjfdsf')
    })
  })
})
