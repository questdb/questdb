package io.questdb.cairo.mv;

import io.questdb.cairo.TableToken;

public class MaterializedViewDefinition {
    private final CharSequence baseTableName;
    private final long intervalMicros;
    private final TableToken matViewToken;
    private final CharSequence query;
    private final long startEpochMicros;

    public MaterializedViewDefinition(CharSequence baseTableName, long startEpochMicros, long intervalMicros, CharSequence query, TableToken matViewToken) {
        this.baseTableName = baseTableName;
        this.startEpochMicros = startEpochMicros;
        this.intervalMicros = intervalMicros;
        this.query = query;
        this.matViewToken = matViewToken;
    }

    public CharSequence getBaseTableName() {
        return baseTableName;
    }

    public long getIntervalMicros() {
        return intervalMicros;
    }

    public CharSequence getQuery() {
        return query;
    }

    public long getStartEpochMicros() {
        return startEpochMicros;
    }

    public TableToken getViewToken() {
        return matViewToken;
    }
}
