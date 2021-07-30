var Client = require('../')
var assert = require('assert')
var semver = require('semver')

describe('version', function () {
  it('is exported', function () {
    assert(Client.version)
    assert.equal(require('../package.json').version, Client.version)
    assert(semver.gt(Client.version, '1.4.0'))
  })
})
