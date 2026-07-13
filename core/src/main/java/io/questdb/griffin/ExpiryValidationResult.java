package io.questdb.griffin;

import io.questdb.std.IntList;

/**
 * Result of binding and classifying an EXPIRE ROWS predicate. The compiler returns the
 * classification with the successful validation so DDL does not compile the predicate again.
 */
public final class ExpiryValidationResult {
    public static final ExpiryValidationResult MONOTONIC = new ExpiryValidationResult(false, true, true, new IntList());
    public static final ExpiryValidationResult NON_MONOTONIC = new ExpiryValidationResult(false, true, false, new IntList());

    private final boolean hasClock;
    private final boolean isDeterministic;
    private final boolean isMonotonic;
    private final IntList referencedColumnIndexes;

    public ExpiryValidationResult(
            boolean hasClock,
            boolean isDeterministic,
            boolean isMonotonic,
            IntList referencedColumnIndexes
    ) {
        this.hasClock = hasClock;
        this.isDeterministic = isDeterministic;
        this.isMonotonic = isMonotonic;
        this.referencedColumnIndexes = referencedColumnIndexes;
    }

    public IntList getReferencedColumnIndexes() {
        return referencedColumnIndexes;
    }

    public boolean hasClock() {
        return hasClock;
    }

    public boolean isDeterministic() {
        return isDeterministic;
    }

    public boolean isMonotonic() {
        return isMonotonic;
    }
}
