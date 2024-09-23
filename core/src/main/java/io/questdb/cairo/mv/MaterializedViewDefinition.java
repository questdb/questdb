package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;

public class MaterializedViewDefinition {
    private final String baseTableName;
    private final long fromMicros;
    private final char intervalQualifier;
    private final int intervalValue;
    private final TableToken matViewToken;
    private final String query;
    private final String timeZone;
    private final String timeZoneOffset;
    private final long toMicros;

    public MaterializedViewDefinition(
            TableToken matViewToken, String query, String baseTableName,
            int intervalValue, char intervalQualifier, long fromMicros, long toMicros,
            String timeZone, String timeZoneOffset
    ) {
        this.matViewToken = matViewToken;
        this.query = query;
        this.baseTableName = baseTableName;
        this.intervalValue = intervalValue;
        this.intervalQualifier = intervalQualifier;
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

    public char getIntervalQualifier() {
        return intervalQualifier;
    }

    public int getIntervalValue() {
        return intervalValue;
    }

    public TableToken getMatViewToken() {
        return matViewToken;
    }

    public String getQuery() {
        return query;
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
