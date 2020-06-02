package io.questdb.cutlass.http;

public interface Retry {
    /**
     * Run a retry
     * @return false if not successful or true if successful
     */
    boolean run();
}
