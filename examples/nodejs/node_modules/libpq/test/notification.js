var Libpq = require('../');
var assert = require('assert');

describe('LISTEN/NOTIFY', function() {
  before(function() {
    this.listener = new Libpq();
    this.notifier = new Libpq();
    this.listener.connectSync();
    this.notifier.connectSync();
  });

  it('works', function() {
    this.notifier.exec("NOTIFY testing, 'My Payload'");
    var notice = this.listener.notifies();
    assert.equal(notice, null);

    this.listener.exec('LISTEN testing');
    this.notifier.exec("NOTIFY testing, 'My Second Payload'");
    this.listener.exec('SELECT NOW()');
    var notice = this.listener.notifies();
    assert(notice, 'listener should have had a notification come in');
    assert.equal(notice.relname, 'testing', 'missing relname == testing');
    assert.equal(notice.extra, 'My Second Payload');
    assert(notice.be_pid);
  });

  after(function() {
    this.listener.finish();
    this.notifier.finish();
  });
});
