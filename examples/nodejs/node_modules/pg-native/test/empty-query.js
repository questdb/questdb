var Client = require('../')
var assert = require('assert')

describe('empty query', () => {
  it('has field metadata in result', (done) => {
    const client = new Client()
    client.connectSync()
    client.query('SELECT NOW() as now LIMIT 0', (err, rows, res) => {
      assert(!err)
      assert.equal(rows.length, 0)
      assert(Array.isArray(res.fields))
      assert.equal(res.fields.length, 1)
      client.end(done)
    })
  })
})
