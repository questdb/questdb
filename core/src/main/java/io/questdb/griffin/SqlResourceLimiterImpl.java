package io.questdb.griffin;

import io.questdb.griffin.engine.LimitOverflowException;

public class SqlResourceLimiterImpl implements SqlResourceLimiter {
    private long maxInMemoryRows;
    private SqlExecutionInterruptor interruptor;

    @Override
    public void checkLimits(long nRows) {
        if (nRows < maxInMemoryRows) {
            interruptor.checkInterrupted();
            return;
        }

        throw LimitOverflowException.instance(maxInMemoryRows);
    }

    public SqlResourceLimiter of(long maxInMemoryRows, SqlExecutionInterruptor interruptor) {
        this.maxInMemoryRows = maxInMemoryRows;
        this.interruptor = null == interruptor ? SqlExecutionInterruptor.NOP_INTERRUPTOR : interruptor;
        return this;
    }
}
