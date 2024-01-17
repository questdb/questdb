package io.questdb.test;

import io.questdb.TelemetryConfiguration;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.test.cairo.ConfigurationOverrides;

public interface TestCairoConfigurationFactory {
    CairoConfiguration getInstance(CharSequence root, TelemetryConfiguration telemetryConfiguration, ConfigurationOverrides overrides, TextConfiguration textConfiguration);
}
