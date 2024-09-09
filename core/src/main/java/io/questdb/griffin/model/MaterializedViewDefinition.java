package io.questdb.griffin.model;

public class MaterializedViewDefinition {
    private final CharSequence baseTableName;
    private final long intervalMicros;
    private final CharSequence query;
    private final long startEpochMicros;

    public MaterializedViewDefinition(CharSequence baseTableName, long startEpochMicros, long intervalMicros, CharSequence query) {
        this.baseTableName = baseTableName;
        this.startEpochMicros = startEpochMicros;
        this.intervalMicros = intervalMicros;
        this.query = query;
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
}
