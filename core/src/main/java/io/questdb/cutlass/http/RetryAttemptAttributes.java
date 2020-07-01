package io.questdb.cutlass.http;

public class RetryAttemptAttributes {
    public long nextRunTimestamp;
    public long lastRunTimestamp;
    public long waitStartTimestamp;
    public int attempt;
}
