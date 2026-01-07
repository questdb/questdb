package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.griffin.model.QueryModel;

public final class LeftOuterJoinOperatorFactory extends AbstractHashJoinOperatorFactory {
    public LeftOuterJoinOperatorFactory() {
        super(QueryModel.JOIN_LEFT_OUTER);
    }
}
