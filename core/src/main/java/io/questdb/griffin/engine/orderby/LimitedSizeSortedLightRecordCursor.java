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

    private final Function loFunction;
    private final Function hiFunction;

    boolean isFirstN;   //true - keep first N rows, false - keep last N rows 
    private long limit; //<0 - limit disabled ; =0 means don't fetch any rows ; >0 - apply limit  
    private long skipFirst; //skip first N rows 
    private long skipLast;  //skip last N rows 

    public LimitedSizeSortedLightRecordCursor(LimitedSizeLongTreeChain chain, RecordComparator comparator, Function loFunction, Function hiFunction) {
        this.chain = chain;
        this.comparator = comparator;
        // assign it once, its the same instance anyway
        this.chainCursor = chain.getCursor();

        this.loFunction = loFunction;
        this.hiFunction = hiFunction;
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
        if (chainCursor.hasNext()) {
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
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
        loFunction.init(base, executionContext);
        if (hiFunction != null) {
            hiFunction.init(base, executionContext);
        }
        base.toTop();
        calculateBounds();

        chain.clear();
        chain.setIsfirstN(isFirstN);//these two are set here because lo, hi must be evaluated first
        chain.setMaxValues(limit);

        this.base = base;
        this.baseRecord = base.getRecord();
        final Record placeHolderRecord = base.getRecordB();
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();

        if (limit != 0) {
            while (base.hasNext()) {
                circuitBreaker.test();  //here
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

        if (skipFirst > 0) {
            chain.skipFirst(skipFirst);
        }

        if (skipLast > 0) {
            chain.skipLast(skipLast);
        }

        chainCursor.toTop();
    }

    /*
     * 1. "limit L" means we only need to keep :
     *      L >=0 - first L records
     *      L < 0 - last L records
     * 2. "limit L, H" means we need to keep :
     *    L < 0          - last  L records (but skip last H records, if H >=0 then don't skip anything    )
     *    L >= 0, H >= 0 - first H records (but skip first L later, if H <= L then return empty set)
     *    L >= 0, H < 0  - we can't optimize this case (because it spans from record L-th from the beginning up to
     *                     H-th from the end, and we don't  ) and need to revert to default behavior -
     *                     produce the whole set and skip .
     *
     *  Similar to LimitRecordCursorFactory.LimitRecordCursor but doesn't check underlying count .
     */
    public void calculateBounds() {
        skipFirst = skipLast = 0;

        long lo = loFunction.getLong(null);
        if (lo < 0 && hiFunction == null) {
            // last N rows
            isFirstN = false;
            // lo is negative, -5 for example
            // if we have 12 records we need to skip 12-5 = 7
            // if we have 4 records = return all of them
            // set limit to return remaining rows
            limit = -lo;
            skipFirst = skipLast = 0;
        } else if (lo > -1 && hiFunction == null) {
            // first N rows
            isFirstN = true;
            limit = lo;
            skipFirst = skipLast = 0;
        } else {
            // at this stage we also have 'hi'
            long hi = hiFunction.getLong(null);
            if (lo < 0) {
                // right, here we are looking for something like -10,-5 five rows away from tail
                if (lo < hi) {
                    isFirstN = false;
                    limit = -lo;
                    skipFirst = 0;
                    skipLast = Math.max(-hi, 0);
                    //}
                } else {
                    // this is invalid bottom range, for example -3, -10
                    limit = 0;//produce empty result
                }
            } else { //lo >= 0
                if (hi < 0) {
                    //if lo>=0 but hi<0 then we fall back to standard algorithm because we can't estimate result size 
                    // (it's from lo up to end-hi so probably whole result anyway )
                    limit = -1;
                    skipFirst = lo;
                    skipLast = -hi;
                } else { //both lo and hi are positive 
                    if (hi <= lo) {
                        limit = 0;//produce empty result
                    } else {
                        isFirstN = true;
                        limit = hi;
                        //but we've to skip to lo
                        skipFirst = lo;
                    }
                }
            }
        }
    }


}

