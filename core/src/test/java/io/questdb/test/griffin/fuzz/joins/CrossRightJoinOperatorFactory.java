package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.griffin.model.QueryModel;

public final class CrossRightJoinOperatorFactory extends AbstractNestedLoopJoinOperatorFactory {
    public CrossRightJoinOperatorFactory() {
        super(QueryModel.JOIN_CROSS_RIGHT);
    }
}
