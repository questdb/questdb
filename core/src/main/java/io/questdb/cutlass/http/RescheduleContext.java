package io.questdb.cutlass.http;

public interface RescheduleContext {
    void reschedule(Retry retry);
}
