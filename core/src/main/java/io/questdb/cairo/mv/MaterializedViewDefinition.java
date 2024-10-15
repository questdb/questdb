package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;

public class MaterializedViewDefinition {
    private final String baseTableName;
    private final long fromMicros;
    private final TableToken matViewToken;
    private final String query;
    private final long samplingInterval;
    private final char samplingIntervalUnit;
    private final String timeZone;
    private final String timeZoneOffset;
    private final long toMicros;

    public MaterializedViewDefinition(
            TableToken matViewToken, String query, String baseTableName,
            long samplingInterval, char samplingIntervalUnit, long fromMicros, long toMicros,
            String timeZone, String timeZoneOffset
    ) {
        this.matViewToken = matViewToken;
        this.query = query;
        this.baseTableName = baseTableName;
        this.samplingInterval = samplingInterval;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.fromMicros = fromMicros;
        this.toMicros = toMicros;
        this.timeZone = timeZone;
        this.timeZoneOffset = timeZoneOffset;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public long getFromMicros() {
        return fromMicros;
    }

    public TableToken getMatViewToken() {
        return matViewToken;
    }

    public String getQuery() {
        return query;
    }

    public long getSamplingInterval() {
        return samplingInterval;
    }

    public char getSamplingIntervalUnit() {
        return samplingIntervalUnit;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public String getTimeZoneOffset() {
        return timeZoneOffset;
    }

    public long getToMicros() {
        return toMicros;
    }
}
