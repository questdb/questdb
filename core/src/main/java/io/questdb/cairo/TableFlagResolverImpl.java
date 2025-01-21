package io.questdb.cairo;

import io.questdb.TelemetryConfigLogger;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.std.Chars;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import org.jetbrains.annotations.NotNull;

public class TableFlagResolverImpl implements TableFlagResolver {
    private final String[] publicTables;
    private final String systemTableNamePrefix;

    public TableFlagResolverImpl(String systemTableNamePrefix) {
        this.systemTableNamePrefix = systemTableNamePrefix;

        publicTables = new String[]{
                systemTableNamePrefix + TelemetryWalTask.TABLE_NAME,
                TelemetryTask.TABLE_NAME,
                TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME
        };
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override
    public boolean isPublic(@NotNull CharSequence tableName) {
        for (int i = 0, n = publicTables.length; i < n; i++) {
            if (Chars.equalsIgnoreCase(tableName, publicTables[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSystem(@NotNull CharSequence tableName) {
        return Chars.startsWith(tableName, systemTableNamePrefix)
                || Chars.equalsIgnoreCase(tableName, TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME)
                || Chars.equalsIgnoreCase(tableName, TelemetryTask.TABLE_NAME)
                || Chars.equalsIgnoreCase(tableName, QueryTracingJob.TABLE_NAME);
    }
}
