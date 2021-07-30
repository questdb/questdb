'use strict'
const net = require('net')
const co = require('co')
const expect = require('expect.js')

const describe = require('mocha').describe
const it = require('mocha').it
const before = require('mocha').before
const after = require('mocha').after

const Pool = require('../')

describe('connection timeout', () => {
  const connectionFailure = new Error('Temporary connection failure')

  before((done) => {
    this.server = net.createServer((socket) => {
      socket.on('data', () => {
        // discard any buffered data or the server wont terminate
      })
    })

    this.server.listen(() => {
      this.port = this.server.address().port
      done()
    })
  })

  after((done) => {
    this.server.close(done)
  })

  it('should callback with an error if timeout is passed', (done) => {
    const pool = new Pool({ connectionTimeoutMillis: 10, port: this.port, host: 'localhost' })
    pool.connect((err, client, release) => {
      expect(err).to.be.an(Error)
      expect(err.message).to.contain('timeout')
      expect(client).to.equal(undefined)
      expect(pool.idleCount).to.equal(0)
      done()
    })
  })

  it('should reject promise with an error if timeout is passed', (done) => {
    const pool = new Pool({ connectionTimeoutMillis: 10, port: this.port, host: 'localhost' })
    pool.connect().catch((err) => {
      expect(err).to.be.an(Error)
      expect(err.message).to.contain('timeout')
      expect(pool.idleCount).to.equal(0)
      done()
    })
  })

  it(
    'should handle multiple timeouts',
    co.wrap(
      function* () {
        const errors = []
        const pool = new Pool({ connectionTimeoutMillis: 1, port: this.port, host: 'localhost' })
        for (var i = 0; i < 15; i++) {
          try {
            yield pool.connect()
          } catch (e) {
            errors.push(e)
          }
        }
        expect(errors).to.have.length(15)
      }.bind(this)
    )
  )

  it('should timeout on checkout of used connection', (done) => {
    const pool = new Pool({ connectionTimeoutMillis: 100, max: 1 })
    pool.connect((err, client, release) => {
      expect(err).to.be(undefined)
      expect(client).to.not.be(undefined)
      pool.connect((err, client) => {
        expect(err).to.be.an(Error)
        expect(client).to.be(undefined)
        release()
        pool.end(done)
      })
    })
  })

  it('should not break further pending checkouts on a timeout', (done) => {
    const pool = new Pool({ connectionTimeoutMillis: 200, max: 1 })
    pool.connect((err, client, releaseOuter) => {
      expect(err).to.be(undefined)

      pool.connect((err, client) => {
        expect(err).to.be.an(Error)
        expect(client).to.be(undefined)
        releaseOuter()
      })

      setTimeout(() => {
        pool.connect((err, client, releaseInner) => {
          expect(err).to.be(undefined)
          expect(client).to.not.be(undefined)
          releaseInner()
          pool.end(done)
        })
      }, 100)
    })
  })

  it('should timeout on query if all clients are busy', (done) => {
    const pool = new Pool({ connectionTimeoutMillis: 100, max: 1 })
    pool.connect((err, client, release) => {
      expect(err).to.be(undefined)
      expect(client).to.not.be(undefined)
      pool.query('select now()', (err, result) => {
        expect(err).to.be.an(Error)
        expect(result).to.be(undefined)
        release()
        pool.end(done)
      })
    })
  })

  it('should recover from timeout errors', (done) => {
    const pool = new Pool({ connectionTimeoutMillis: 100, max: 1 })
    pool.connect((err, client, release) => {
      expect(err).to.be(undefined)
      expect(client).to.not.be(undefined)
      pool.query('select now()', (err, result) => {
        expect(err).to.be.an(Error)
        expect(result).to.be(undefined)
        release()
        pool.query('select $1::text as name', ['brianc'], (err, res) => {
          expect(err).to.be(undefined)
          expect(res.rows).to.have.length(1)
          pool.end(done)
        })
      })
    })
  })

  it('continues processing after a connection failure', (done) => {
    const Client = require('pg').Client
    const orgConnect = Client.prototype.connect
    let called = false

    Client.prototype.connect = function (cb) {
      // Simulate a failure on first call
      if (!called) {
        called = true

        return setTimeout(() => {
          cb(connectionFailure)
        }, 100)
      }
      // And pass-through the second call
      orgConnect.call(this, cb)
    }

    const pool = new Pool({
      Client: Client,
      connectionTimeoutMillis: 1000,
      max: 1,
    })

    pool.connect((err, client, release) => {
      expect(err).to.be(connectionFailure)

      pool.query('select $1::text as name', ['brianc'], (err, res) => {
        expect(err).to.be(undefined)
        expect(res.rows).to.have.length(1)
        pool.end(done)
      })
    })
  })

  it('releases newly connected clients if the queued already timed out', (done) => {
    const Client = require('pg').Client

    const orgConnect = Client.prototype.connect

    let connection = 0

    Client.prototype.connect = function (cb) {
      // Simulate a failure on first call
      if (connection === 0) {
        connection++

        return setTimeout(() => {
          cb(connectionFailure)
        }, 300)
      }

      // And second connect taking > connection timeout
      if (connection === 1) {
        connection++

        return setTimeout(() => {
          orgConnect.call(this, cb)
        }, 1000)
      }

      orgConnect.call(this, cb)
    }

    const pool = new Pool({
      Client: Client,
      connectionTimeoutMillis: 1000,
      max: 1,
    })

    // Direct connect
    pool.connect((err, client, release) => {
      expect(err).to.be(connectionFailure)
    })

    // Queued
    let called = 0
    pool.connect((err, client, release) => {
      // Verify the callback is only called once
      expect(called++).to.be(0)
      expect(err).to.be.an(Error)

      pool.query('select $1::text as name', ['brianc'], (err, res) => {
        expect(err).to.be(undefined)
        expect(res.rows).to.have.length(1)
        pool.end(done)
      })
    })
  })
})
