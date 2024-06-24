package io.questdb.test.cairo;

import io.questdb.TelemetryConfigLogger;
import io.questdb.cairo.TableFlagResolver;
import io.questdb.cairo.TableFlagResolverImpl;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableFlagsResolverTest {
    final TableFlagResolver flags = new TableFlagResolverImpl("sys.");

    @Test
    public void testPublicTables() {
        assertTrue(flags.isPublic(TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME));
        assertTrue(flags.isPublic(TelemetryTask.TABLE_NAME));
        assertTrue(flags.isPublic("sys." + TelemetryWalTask.TABLE_NAME));

        assertTrue(flags.isPublic(TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME.toLowerCase()));
        assertTrue(flags.isPublic(TelemetryTask.TABLE_NAME.toUpperCase()));
        assertTrue(flags.isPublic("SYS." + TelemetryWalTask.TABLE_NAME));

        assertFalse(flags.isPublic("anything"));
        assertFalse(flags.isPublic("sys.anything"));
    }

    @Test
    public void testSysTables() {
        assertTrue(flags.isSystem(TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME));
        assertTrue(flags.isSystem(TelemetryTask.TABLE_NAME));
        assertTrue(flags.isSystem("sys." + TelemetryWalTask.TABLE_NAME));

        assertTrue(flags.isSystem(TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME.toUpperCase()));
        assertTrue(flags.isSystem(TelemetryTask.TABLE_NAME.toLowerCase()));

        assertTrue(flags.isSystem("sys.anything"));
        assertTrue(flags.isSystem("sys."));

        assertFalse(flags.isSystem("anything"));
    }
}
