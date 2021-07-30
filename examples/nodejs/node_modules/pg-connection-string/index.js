'use strict'

var url = require('url')
var fs = require('fs')

//Parse method copied from https://github.com/brianc/node-postgres
//Copyright (c) 2010-2014 Brian Carlson (brian.m.carlson@gmail.com)
//MIT License

//parses a connection string
function parse(str) {
  //unix socket
  if (str.charAt(0) === '/') {
    var config = str.split(' ')
    return { host: config[0], database: config[1] }
  }

  // url parse expects spaces encoded as %20
  var result = url.parse(
    / |%[^a-f0-9]|%[a-f0-9][^a-f0-9]/i.test(str) ? encodeURI(str).replace(/\%25(\d\d)/g, '%$1') : str,
    true
  )
  var config = result.query
  for (var k in config) {
    if (Array.isArray(config[k])) {
      config[k] = config[k][config[k].length - 1]
    }
  }

  var auth = (result.auth || ':').split(':')
  config.user = auth[0]
  config.password = auth.splice(1).join(':')

  config.port = result.port
  if (result.protocol == 'socket:') {
    config.host = decodeURI(result.pathname)
    config.database = result.query.db
    config.client_encoding = result.query.encoding
    return config
  }
  if (!config.host) {
    // Only set the host if there is no equivalent query param.
    config.host = result.hostname
  }

  // If the host is missing it might be a URL-encoded path to a socket.
  var pathname = result.pathname
  if (!config.host && pathname && /^%2f/i.test(pathname)) {
    var pathnameSplit = pathname.split('/')
    config.host = decodeURIComponent(pathnameSplit[0])
    pathname = pathnameSplit.splice(1).join('/')
  }
  // result.pathname is not always guaranteed to have a '/' prefix (e.g. relative urls)
  // only strip the slash if it is present.
  if (pathname && pathname.charAt(0) === '/') {
    pathname = pathname.slice(1) || null
  }
  config.database = pathname && decodeURI(pathname)

  if (config.ssl === 'true' || config.ssl === '1') {
    config.ssl = true
  }

  if (config.ssl === '0') {
    config.ssl = false
  }

  if (config.sslcert || config.sslkey || config.sslrootcert || config.sslmode) {
    config.ssl = {}
  }

  if (config.sslcert) {
    config.ssl.cert = fs.readFileSync(config.sslcert).toString()
  }

  if (config.sslkey) {
    config.ssl.key = fs.readFileSync(config.sslkey).toString()
  }

  if (config.sslrootcert) {
    config.ssl.ca = fs.readFileSync(config.sslrootcert).toString()
  }

  switch (config.sslmode) {
    case 'disable': {
      config.ssl = false
      break
    }
    case 'prefer':
    case 'require':
    case 'verify-ca':
    case 'verify-full': {
      break
    }
    case 'no-verify': {
      config.ssl.rejectUnauthorized = false
      break
    }
  }

  return config
}

module.exports = parse

parse.parse = parse
