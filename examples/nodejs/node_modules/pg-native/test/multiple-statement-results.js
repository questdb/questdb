var Client = require('../')
var assert = require('assert')

describe('multiple statements', () => {
  before(() => {
    this.client = new Client()
    this.client.connectSync()
  })

  after(() => this.client.end())

  it('works with multiple queries', (done) => {
    const text = `
    SELECT generate_series(1, 2) as foo;
    SELECT generate_series(10, 11) as bar;
    SELECT generate_series(20, 22) as baz;
    `
    this.client.query(text, (err, results) => {
      if (err) return done(err)
      assert(Array.isArray(results))
      assert.equal(results.length, 3)
      assert(Array.isArray(results[0]))
      assert(Array.isArray(results[1]))
      assert(Array.isArray(results[2]))
      done()
    })
  })
})
