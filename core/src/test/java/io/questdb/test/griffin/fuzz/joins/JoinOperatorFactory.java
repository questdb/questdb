package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Rnd;

public interface JoinOperatorFactory {
    /**
     * Whether this join can be followed by another join in a generated pipeline.
     */
    boolean canChain();

    /**
     * Scan-direction requirements for the left/right inputs.
     */
    JoinScanRequirement scanRequirement();

    /**
     * Builds under-test and oracle factories for the given join inputs.
     */
    FuzzFactories build(FuzzBuildContext context, JoinInputs inputs, Rnd rnd, String slaveAlias);
}
