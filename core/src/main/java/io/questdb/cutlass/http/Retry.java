package io.questdb.cutlass.http;

@FunctionalInterface
public interface Retry {
    /**
     * Run a retry
     * @return false if not successful or true if successful
     */
    boolean tryRerun(HttpRequestProcessorSelector selector);
}
