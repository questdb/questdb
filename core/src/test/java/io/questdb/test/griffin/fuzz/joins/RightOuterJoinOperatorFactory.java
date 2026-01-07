package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.griffin.model.QueryModel;

public final class RightOuterJoinOperatorFactory extends AbstractHashJoinOperatorFactory {
    public RightOuterJoinOperatorFactory() {
        super(QueryModel.JOIN_RIGHT_OUTER);
    }

    @Override
    public boolean canChain() {
        return false;
    }
}
