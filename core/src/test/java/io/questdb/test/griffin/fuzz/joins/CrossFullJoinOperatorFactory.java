package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.griffin.model.QueryModel;

public final class CrossFullJoinOperatorFactory extends AbstractNestedLoopJoinOperatorFactory {
    public CrossFullJoinOperatorFactory() {
        super(QueryModel.JOIN_CROSS_FULL);
    }
}
