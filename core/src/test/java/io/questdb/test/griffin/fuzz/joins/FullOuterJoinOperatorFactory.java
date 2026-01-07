package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.griffin.model.QueryModel;

public final class FullOuterJoinOperatorFactory extends AbstractHashJoinOperatorFactory {
    public FullOuterJoinOperatorFactory() {
        super(QueryModel.JOIN_FULL_OUTER);
    }

    @Override
    public boolean canChain() {
        return false;
    }
}
