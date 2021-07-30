var assert = require('assert')
var Client = require('../')
var concat = require('concat-stream')
var _ = require('lodash')

describe('COPY TO', function () {
  before(function (done) {
    this.client = Client()
    this.client.connect(done)
  })

  after(function (done) {
    this.client.end(done)
  })

  it('works - basic check', function (done) {
    var limit = 1000
    var qText = 'COPY (SELECT * FROM generate_series(0, ' + (limit - 1) + ')) TO stdout'
    var self = this
    this.client.query(qText, function (err) {
      if (err) return done(err)
      var stream = self.client.getCopyStream()
      // pump the stream for node v0.11.x
      stream.read()
      stream.pipe(concat(function (buff) {
        var res = buff.toString('utf8')
        var expected = _.range(0, limit).join('\n') + '\n'
        assert.equal(res, expected)
        done()
      }))
    })
  })
})
