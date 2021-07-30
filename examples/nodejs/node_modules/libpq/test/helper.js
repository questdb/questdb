var PQ = require('../');

var createTable = function(pq) {
  pq.exec('CREATE TEMP TABLE test_data(name text, age int)')
  console.log(pq.resultErrorMessage());
  pq.exec("INSERT INTO test_data(name, age) VALUES ('brian', 32), ('aaron', 30), ('', null);")
};

module.exports = {
  setupIntegration: function() {
    before(function() {
      this.pq = new PQ();
      this.pq.connectSync();
      createTable(this.pq);
    });

    after(function() {
      this.pq.finish();
    });
  }
};
