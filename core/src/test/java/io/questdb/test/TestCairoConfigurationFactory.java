package io.questdb.test;

import io.questdb.TelemetryConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.test.cairo.Overrides;

public interface TestCairoConfigurationFactory {
    CairoConfiguration getInstance(CharSequence root, TelemetryConfiguration telemetryConfiguration, Overrides overrides);
}
