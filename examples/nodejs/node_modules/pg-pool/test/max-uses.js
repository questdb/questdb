const expect = require('expect.js')
const co = require('co')
const _ = require('lodash')

const describe = require('mocha').describe
const it = require('mocha').it

const Pool = require('../')

describe('maxUses', () => {
  it(
    'can create a single client and use it once',
    co.wrap(function* () {
      const pool = new Pool({ maxUses: 2 })
      expect(pool.waitingCount).to.equal(0)
      const client = yield pool.connect()
      const res = yield client.query('SELECT $1::text as name', ['hi'])
      expect(res.rows[0].name).to.equal('hi')
      client.release()
      pool.end()
    })
  )

  it(
    'getting a connection a second time returns the same connection and releasing it also closes it',
    co.wrap(function* () {
      const pool = new Pool({ maxUses: 2 })
      expect(pool.waitingCount).to.equal(0)
      const client = yield pool.connect()
      client.release()
      const client2 = yield pool.connect()
      expect(client).to.equal(client2)
      expect(client2._ending).to.equal(false)
      client2.release()
      expect(client2._ending).to.equal(true)
      return yield pool.end()
    })
  )

  it(
    'getting a connection a third time returns a new connection',
    co.wrap(function* () {
      const pool = new Pool({ maxUses: 2 })
      expect(pool.waitingCount).to.equal(0)
      const client = yield pool.connect()
      client.release()
      const client2 = yield pool.connect()
      expect(client).to.equal(client2)
      client2.release()
      const client3 = yield pool.connect()
      expect(client3).not.to.equal(client2)
      client3.release()
      return yield pool.end()
    })
  )

  it(
    'getting a connection from a pending request gets a fresh client when the released candidate is expended',
    co.wrap(function* () {
      const pool = new Pool({ max: 1, maxUses: 2 })
      expect(pool.waitingCount).to.equal(0)
      const client1 = yield pool.connect()
      pool.connect().then((client2) => {
        expect(client2).to.equal(client1)
        expect(pool.waitingCount).to.equal(1)
        // Releasing the client this time should also expend it since maxUses is 2, causing client3 to be a fresh client
        client2.release()
      })
      const client3Promise = pool.connect().then((client3) => {
        // client3 should be a fresh client since client2's release caused the first client to be expended
        expect(pool.waitingCount).to.equal(0)
        expect(client3).not.to.equal(client1)
        return client3.release()
      })
      // There should be two pending requests since we have 3 connect requests but a max size of 1
      expect(pool.waitingCount).to.equal(2)
      // Releasing the client should not yet expend it since maxUses is 2
      client1.release()
      yield client3Promise
      return yield pool.end()
    })
  )

  it(
    'logs when removing an expended client',
    co.wrap(function* () {
      const messages = []
      const log = function (msg) {
        messages.push(msg)
      }
      const pool = new Pool({ maxUses: 1, log })
      const client = yield pool.connect()
      client.release()
      expect(messages).to.contain('remove expended client')
      return yield pool.end()
    })
  )
})
