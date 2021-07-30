const Pool = require('../')

const expect = require('expect.js')
const net = require('net')

describe('releasing clients', () => {
  it('removes a client which cannot be queried', async () => {
    // make a pool w/ only 1 client
    const pool = new Pool({ max: 1 })
    expect(pool.totalCount).to.eql(0)
    const client = await pool.connect()
    expect(pool.totalCount).to.eql(1)
    expect(pool.idleCount).to.eql(0)
    // reach into the client and sever its connection
    client.connection.end()

    // wait for the client to error out
    const err = await new Promise((resolve) => client.once('error', resolve))
    expect(err).to.be.ok()
    expect(pool.totalCount).to.eql(1)
    expect(pool.idleCount).to.eql(0)

    // try to return it to the pool - this removes it because its broken
    client.release()
    expect(pool.totalCount).to.eql(0)
    expect(pool.idleCount).to.eql(0)

    // make sure pool still works
    const { rows } = await pool.query('SELECT NOW()')
    expect(rows).to.have.length(1)
    await pool.end()
  })

  it('removes a client which is ending', async () => {
    // make a pool w/ only 1 client
    const pool = new Pool({ max: 1 })
    expect(pool.totalCount).to.eql(0)
    const client = await pool.connect()
    expect(pool.totalCount).to.eql(1)
    expect(pool.idleCount).to.eql(0)
    // end the client gracefully (but you shouldn't do this with pooled clients)
    client.end()

    // try to return it to the pool
    client.release()
    expect(pool.totalCount).to.eql(0)
    expect(pool.idleCount).to.eql(0)

    // make sure pool still works
    const { rows } = await pool.query('SELECT NOW()')
    expect(rows).to.have.length(1)
    await pool.end()
  })
})
