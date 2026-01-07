package io.questdb.test.griffin.fuzz;

public final class JoinScanRequirement {
    private final ScanDirectionRequirement left;
    private final ScanDirectionRequirement right;

    /**
     * Captures left/right scan-direction requirements for a join operator.
     */
    public JoinScanRequirement(ScanDirectionRequirement left, ScanDirectionRequirement right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Requirement for the left input.
     */
    public ScanDirectionRequirement left() {
        return left;
    }

    /**
     * Requirement for the right input.
     */
    public ScanDirectionRequirement right() {
        return right;
    }
}
