var Client = require('../')
var ok = require('okay')
var assert = require('assert')
var concat = require('concat-stream')

describe('async workflow', function () {
  before(function (done) {
    this.client = new Client()
    this.client.connect(done)
  })

  var echoParams = function (params, cb) {
    this.client.query('SELECT $1::text as first, $2::text as second', params, ok(cb, function (rows) {
      checkParams(params, rows)
      cb(null, rows)
    }))
  }

  var checkParams = function (params, rows) {
    assert.equal(rows.length, 1)
    assert.equal(rows[0].first, params[0])
    assert.equal(rows[0].second, params[1])
  }

  it('sends async query', function (done) {
    var params = ['one', 'two']
    echoParams.call(this, params, done)
  })

  it('sends multiple async queries', function (done) {
    var self = this
    var params = ['bang', 'boom']
    echoParams.call(this, params, ok(done, function (rows) {
      echoParams.call(self, params, done)
    }))
  })

  it('sends an async query, copies in, copies out, and sends another query', function (done) {
    var self = this
    this.client.querySync('CREATE TEMP TABLE test(name text, age int)')
    this.client.query("INSERT INTO test(name, age) VALUES('brian', 32)", ok(done, function () {
      self.client.querySync('COPY test FROM stdin')
      var input = self.client.getCopyStream()
      input.write(Buffer.from('Aaron\t30\n', 'utf8'))
      input.end(function () {
        self.client.query('SELECT COUNT(*) FROM test', ok(done, function (rows) {
          assert.equal(rows.length, 1)
          self.client.query('COPY test TO stdout', ok(done, function () {
            var output = self.client.getCopyStream()

            // pump the stream
            output.read()
            output.pipe(concat(function (res) {
              done()
            }))
          }))
        }))
      })
    }))
  })
})
