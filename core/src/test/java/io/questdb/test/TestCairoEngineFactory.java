package io.questdb.test;

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;

public interface TestCairoEngineFactory {
    CairoEngine getInstance(CairoConfiguration configuration, Metrics metrics);
}
