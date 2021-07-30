var Libpq = require('../');
var _ = require('lodash');
var assert = require('assert');

describe('connectSync', function() {
  it('works 50 times in a row', function() {
    var pqs = _.times(50, function() {
      return new Libpq();
    });
    pqs.forEach(function(pq) {
      pq.connectSync();
    });
    pqs.forEach(function(pq) {
      pq.finish();
    });
  });
});

//doing a bunch of stuff here to
//try and shake out a hard to track down segfault
describe('connect async', function() {
  var total = 50;
  it('works ' + total + ' times in a row', function(done) {
    var pqs = _.times(total, function() {
      return new Libpq();
    });

    var count = 0;
    var connect = function(cb) {
      pqs.forEach(function(pq) {
        pq.connect(function(err) {
          assert(!err);
          count++;
          pq.startReader();
          if(count == total) {
            cb();
          }
        });
      });
    };
    connect(function() {
      pqs.forEach(function(pq) {
        pq.stopReader();
        pq.finish();
      });
      done();
    });
  });
});
