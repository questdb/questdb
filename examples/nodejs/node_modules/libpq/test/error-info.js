var Libpq = require('../');
var assert = require('assert');
var helper = require('./helper');

describe('error info', function() {
  helper.setupIntegration();

  describe('when there is no error', function() {

    it('everything is null', function() {
      var pq = this.pq;
      pq.exec('SELECT NOW()');
      assert(!pq.errorMessage(), pq.errorMessage());
      assert.equal(pq.ntuples(), 1);
      assert(pq.resultErrorFields(), null);
    });
  });

  describe('when there is an error', function() {

    it('sets all error codes', function() {
      var pq = this.pq;
      pq.exec('INSERT INTO test_data VALUES(1, NOW())');
      assert(pq.errorMessage());
      var err = pq.resultErrorFields();
      assert.notEqual(err, null);
      assert.equal(err.severity, 'ERROR');
      assert.equal(err.sqlState, 42804);
      assert.equal(err.messagePrimary, 'column "age" is of type integer but expression is of type timestamp with time zone');
      assert.equal(err.messageDetail, undefined);
      assert.equal(err.messageHint, 'You will need to rewrite or cast the expression.');
      assert.equal(err.statementPosition, 33);
      assert.equal(err.internalPosition, undefined);
      assert.equal(err.internalQuery, undefined);
      assert.equal(err.context, undefined);
      assert.equal(err.schemaName, undefined);
      assert.equal(err.tableName, undefined);
      assert.equal(err.dataTypeName, undefined);
      assert.equal(err.constraintName, undefined);
      assert.equal(err.sourceFile, "parse_target.c");
      assert(parseInt(err.sourceLine));
      assert.equal(err.sourceFunction, "transformAssignedExpr");
    });
  });
});
