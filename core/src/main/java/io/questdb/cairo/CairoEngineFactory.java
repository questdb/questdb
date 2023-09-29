package io.questdb.cairo;

import io.questdb.Metrics;

@FunctionalInterface
public interface CairoEngineFactory {
    CairoEngine createInstance(CairoConfiguration configuration, Metrics metrics);
}
