package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.griffin.model.QueryModel;

public final class InnerJoinOperatorFactory extends AbstractHashJoinOperatorFactory {
    public InnerJoinOperatorFactory() {
        super(QueryModel.JOIN_INNER);
    }
}
