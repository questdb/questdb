package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;

public class MaterializedViewDefinition {
    private final CharSequence baseTableName;
    private final long fromMicros;
    private final char intervalQualifier;
    private final int intervalValue;
    private final TableToken matViewToken;
    private final CharSequence query;
    private final CharSequence timeZone;
    private final CharSequence timeZoneOffset;
    private final long toMicros;

    public MaterializedViewDefinition(
            TableToken matViewToken, CharSequence query, CharSequence baseTableName,
            int intervalValue, char intervalQualifier, long fromMicros, long toMicros,
            CharSequence timeZone, CharSequence timeZoneOffset
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

    public CharSequence getBaseTableName() {
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

    public CharSequence getQuery() {
        return query;
    }

    public CharSequence getTimeZone() {
        return timeZone;
    }

    public CharSequence getTimeZoneOffset() {
        return timeZoneOffset;
    }

    public long getToMicros() {
        return toMicros;
    }
}
