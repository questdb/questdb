'use strict'
const net = require('net')
const co = require('co')
const expect = require('expect.js')

const describe = require('mocha').describe
const it = require('mocha').it

const Pool = require('../')

describe('pool error handling', function () {
  it('Should complete these queries without dying', function (done) {
    const pool = new Pool()
    let errors = 0
    let shouldGet = 0
    function runErrorQuery() {
      shouldGet++
      return new Promise(function (resolve, reject) {
        pool
          .query("SELECT 'asd'+1 ")
          .then(function (res) {
            reject(res) // this should always error
          })
          .catch(function (err) {
            errors++
            resolve(err)
          })
      })
    }
    const ps = []
    for (let i = 0; i < 5; i++) {
      ps.push(runErrorQuery())
    }
    Promise.all(ps).then(function () {
      expect(shouldGet).to.eql(errors)
      pool.end(done)
    })
  })

  describe('calling release more than once', () => {
    it(
      'should throw each time',
      co.wrap(function* () {
        const pool = new Pool()
        const client = yield pool.connect()
        client.release()
        expect(() => client.release()).to.throwError()
        expect(() => client.release()).to.throwError()
        return yield pool.end()
      })
    )

    it('should throw each time with callbacks', function (done) {
      const pool = new Pool()

      pool.connect(function (err, client, clientDone) {
        expect(err).not.to.be.an(Error)
        clientDone()

        expect(() => clientDone()).to.throwError()
        expect(() => clientDone()).to.throwError()

        pool.end(done)
      })
    })
  })

  describe('using an ended pool', () => {
    it('rejects all additional promises', (done) => {
      const pool = new Pool()
      const promises = []
      pool.end().then(() => {
        const squash = (promise) => promise.catch((e) => 'okay!')
        promises.push(squash(pool.connect()))
        promises.push(squash(pool.query('SELECT NOW()')))
        promises.push(squash(pool.end()))
        Promise.all(promises).then((res) => {
          expect(res).to.eql(['okay!', 'okay!', 'okay!'])
          done()
        })
      })
    })

    it('returns an error on all additional callbacks', (done) => {
      const pool = new Pool()
      pool.end(() => {
        pool.query('SELECT *', (err) => {
          expect(err).to.be.an(Error)
          pool.connect((err) => {
            expect(err).to.be.an(Error)
            pool.end((err) => {
              expect(err).to.be.an(Error)
              done()
            })
          })
        })
      })
    })
  })

  describe('error from idle client', () => {
    it(
      'removes client from pool',
      co.wrap(function* () {
        const pool = new Pool()
        const client = yield pool.connect()
        expect(pool.totalCount).to.equal(1)
        expect(pool.waitingCount).to.equal(0)
        expect(pool.idleCount).to.equal(0)
        client.release()
        yield new Promise((resolve, reject) => {
          process.nextTick(() => {
            let poolError
            pool.once('error', (err) => {
              poolError = err
            })

            let clientError
            client.once('error', (err) => {
              clientError = err
            })

            client.emit('error', new Error('expected'))

            expect(clientError.message).to.equal('expected')
            expect(poolError.message).to.equal('expected')
            expect(pool.idleCount).to.equal(0)
            expect(pool.totalCount).to.equal(0)
            pool.end().then(resolve, reject)
          })
        })
      })
    )
  })

  describe('error from in-use client', () => {
    it(
      'keeps the client in the pool',
      co.wrap(function* () {
        const pool = new Pool()
        const client = yield pool.connect()
        expect(pool.totalCount).to.equal(1)
        expect(pool.waitingCount).to.equal(0)
        expect(pool.idleCount).to.equal(0)

        yield new Promise((resolve, reject) => {
          process.nextTick(() => {
            let poolError
            pool.once('error', (err) => {
              poolError = err
            })

            let clientError
            client.once('error', (err) => {
              clientError = err
            })

            client.emit('error', new Error('expected'))

            expect(clientError.message).to.equal('expected')
            expect(poolError).not.to.be.ok()
            expect(pool.idleCount).to.equal(0)
            expect(pool.totalCount).to.equal(1)
            client.release()
            pool.end().then(resolve, reject)
          })
        })
      })
    )
  })

  describe('passing a function to pool.query', () => {
    it('calls back with error', (done) => {
      const pool = new Pool()
      console.log('passing fn to query')
      pool.query((err) => {
        expect(err).to.be.an(Error)
        pool.end(done)
      })
    })
  })

  describe('pool with lots of errors', () => {
    it(
      'continues to work and provide new clients',
      co.wrap(function* () {
        const pool = new Pool({ max: 1 })
        const errors = []
        for (var i = 0; i < 20; i++) {
          try {
            yield pool.query('invalid sql')
          } catch (err) {
            errors.push(err)
          }
        }
        expect(errors).to.have.length(20)
        expect(pool.idleCount).to.equal(0)
        expect(pool.query).to.be.a(Function)
        const res = yield pool.query('SELECT $1::text as name', ['brianc'])
        expect(res.rows).to.have.length(1)
        expect(res.rows[0].name).to.equal('brianc')
        return pool.end()
      })
    )
  })

  it('should continue with queued items after a connection failure', (done) => {
    const closeServer = net
      .createServer((socket) => {
        socket.destroy()
      })
      .unref()

    closeServer.listen(() => {
      const pool = new Pool({ max: 1, port: closeServer.address().port, host: 'localhost' })
      pool.connect((err) => {
        expect(err).to.be.an(Error)
        if (err.code) {
          expect(err.code).to.be('ECONNRESET')
        }
      })
      pool.connect((err) => {
        expect(err).to.be.an(Error)
        if (err.code) {
          expect(err.code).to.be('ECONNRESET')
        }
        closeServer.close(() => {
          pool.end(done)
        })
      })
    })
  })

  it('handles post-checkout client failures in pool.query', (done) => {
    const pool = new Pool({ max: 1 })
    pool.on('error', () => {
      // We double close the connection in this test, prevent exception caused by that
    })
    pool.query('SELECT pg_sleep(5)', [], (err) => {
      expect(err).to.be.an(Error)
      done()
    })

    setTimeout(() => {
      pool._clients[0].end()
    }, 1000)
  })
})
