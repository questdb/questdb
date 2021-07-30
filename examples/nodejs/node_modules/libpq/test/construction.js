var PQ = require('../');
var async = require('async');

describe('Constructing multiple', function() {
  it('works all at once', function() {
    for(var i = 0; i < 1000; i++) {
      var pq = new PQ();
    }
  });

  it('connects and disconnects each client', function(done) {
    var connect = function(n, cb) {
      var pq = new PQ();
      pq.connect(cb);
    };
    async.times(30, connect, done);
  });
})
