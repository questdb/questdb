/*******************************************************************************
 *  Custom composite key creator
 ******************************************************************************/

 package io.questdb;
 
 import io.questdb.cairo.TableToken;
 import java.util.Objects;

 class TableTokenTimestampKey {
    private final TableToken tableToken;
    private final long timestampMicros;


    public TableTokenTimestampKey(TableToken tableToken, long timestampMicros) {
        this.tableToken = tableToken;
        this.timestampMicros = timestampMicros;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public long getTimestampMacro() {
        return timestampMicros;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableTokenTimestampKey that = (TableTokenTimestampKey) o;
        return timestampMicros == that.timestampMicros && Objects.equals(tableToken, that.tableToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableToken, timestampMicros);
    }

 }