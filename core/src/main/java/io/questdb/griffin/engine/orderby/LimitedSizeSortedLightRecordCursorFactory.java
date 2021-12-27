package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;

/**
 * Same as SortedLightRecordCursorFactory but using LimitedSizeLongTreeChain instead.
 */
public class LimitedSizeSortedLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final LimitedSizeLongTreeChain chain;
    private final LimitedSizeSortedLightRecordCursor cursor;
    private final Function loFunction;
    private final Function hiFunction;

    public LimitedSizeSortedLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory base,
            RecordComparator comparator,
            Function loFunc,
            Function hiFunc
    ) {
        super(metadata);
        loFunction = loFunc;
        hiFunction = hiFunc;

        this.chain = new LimitedSizeLongTreeChain(
                configuration.getSqlSortKeyPageSize(),
                configuration.getSqlSortKeyMaxPages(),
                configuration.getSqlSortLightValuePageSize(),
                configuration.getSqlSortLightValueMaxPages());
        this.base = base;
        this.cursor = new LimitedSizeSortedLightRecordCursor(chain, comparator, loFunction, hiFunction);
    }

    @Override
    public void close() {
        base.close();
        chain.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (RuntimeException ex) {
            baseCursor.close();
            throw ex;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean implementsLimit() {
        return true;
    }
}

