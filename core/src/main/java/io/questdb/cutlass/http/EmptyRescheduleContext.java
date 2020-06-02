package io.questdb.cutlass.http;

public final class EmptyRescheduleContext implements RescheduleContext {
    public static final RescheduleContext instance = new EmptyRescheduleContext();

    private EmptyRescheduleContext() {}

    @Override
    public void reschedule(Retry retry) {
    }
}
