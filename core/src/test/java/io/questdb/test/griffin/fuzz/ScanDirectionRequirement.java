package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;

public enum ScanDirectionRequirement {
    NONE,
    ANY,
    FORWARD,
    BACKWARD;

    /**
     * Returns true when this requirement needs a designated timestamp to enforce ordering.
     */
    public boolean requiresTimestamp() {
        return this != NONE;
    }

    /**
     * Returns true when the input must be ordered to satisfy the scan direction.
     */
    public boolean requiresOrdering() {
        return this == FORWARD || this == BACKWARD;
    }

    /**
     * Maps this requirement to the engine scan direction constant.
     */
    public int toScanDirection() {
        return switch (this) {
            case FORWARD -> RecordCursorFactory.SCAN_DIRECTION_FORWARD;
            case BACKWARD -> RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
            default -> RecordCursorFactory.SCAN_DIRECTION_OTHER;
        };
    }
}
