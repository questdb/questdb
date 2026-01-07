package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.griffin.model.QueryModel;

public final class CrossLeftJoinOperatorFactory extends AbstractNestedLoopJoinOperatorFactory {
    public CrossLeftJoinOperatorFactory() {
        super(QueryModel.JOIN_CROSS_LEFT);
    }
}
