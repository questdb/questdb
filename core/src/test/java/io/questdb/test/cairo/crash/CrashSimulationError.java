package io.questdb.test.cairo.crash;

/**
 * Thrown by {@link CrashFaultFilesFacade} to unwind the stack at a chosen durability op,
 * simulating a power loss mid-commit. Extends Error so production handlers that catch
 * {@code CairoException} or {@code Exception} do not absorb it; only the harness driver catches it.
 */
public class CrashSimulationError extends Error {
    public CrashSimulationError(int durabilityOp) {
        super("simulated crash at durability op " + durabilityOp);
    }
}
