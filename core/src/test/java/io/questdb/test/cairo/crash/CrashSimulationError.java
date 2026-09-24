package io.questdb.test.cairo.crash;

/**
 * Thrown by {@link CrashFaultFilesFacade} to unwind the stack at a chosen durability op,
 * simulating a power loss mid-commit. Extends Error (not Exception) so production
 * {@code catch (CairoException)} / {@code catch (Throwable)} handlers do not absorb it;
 * only the harness driver catches it.
 */
public class CrashSimulationError extends Error {
    public CrashSimulationError(int durabilityOp) {
        super("simulated crash at durability op " + durabilityOp);
    }
}
