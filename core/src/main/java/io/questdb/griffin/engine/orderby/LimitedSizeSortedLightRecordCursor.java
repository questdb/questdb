package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;

/**
 * SortedLightRecordCursor implementing LIMIT clause .
 */
public class LimitedSizeSortedLightRecordCursor implements DelegatingRecordCursor {

    private final LimitedSizeLongTreeChain chain;
    private final RecordComparator comparator;
    private final LimitedSizeLongTreeChain.TreeCursor chainCursor;

    private RecordCursor base;
    private Record baseRecord;

    private final long limit; //<0 - limit disabled ; =0 means don't fetch any rows ; >0 - apply limit
    private final long skipFirst; //skip first N rows
    private final long skipLast;  //skip last N rows
    private long rowsLeft;

    public LimitedSizeSortedLightRecordCursor(LimitedSizeLongTreeChain chain, RecordComparator comparator, long limit, long skipFirst, long skipLast) {
        this.chain = chain;
        this.comparator = comparator;
        this.chainCursor = chain.getCursor();
        this.limit = limit;
        this.skipFirst = skipFirst;
        this.skipLast = skipLast;
    }

    @Override
    public void close() {
        chain.clear();
        base.close();
    }

    @Override
    public long size() {
        return Math.max(chain.size() - skipFirst - skipLast, 0);
    }

    @Override
    public Record getRecord() {
        return baseRecord;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (rowsLeft-- > 0 && chainCursor.hasNext()) {
            base.recordAt(baseRecord, chainCursor.next());
            return true;
        }
        return false;
    }

    @Override
    public Record getRecordB() {
        return base.getRecordB();
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(record, atRowId);
    }

    @Override
    public void toTop() {
        chainCursor.toTop();

        long skipLeft = skipFirst;
        while (skipLeft-- > 0 && chainCursor.hasNext()) {
            chainCursor.next();
        }

        rowsLeft = Math.max(chain.size() - skipFirst - skipLast, 0);
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
        this.base = base;
        this.baseRecord = base.getRecord();
        final Record placeHolderRecord = base.getRecordB();
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();

        chain.clear();
        if (limit != 0) {
            while (base.hasNext()) {
                circuitBreaker.test();
                // Tree chain is liable to re-position record to
                // other rows to do record comparison. We must use our
                // own record instance in case base cursor keeps
                // state in the record it returns.
                chain.put(
                        baseRecord,
                        base,
                        placeHolderRecord,
                        comparator
                );
            }
        }

        toTop();
    }
}

