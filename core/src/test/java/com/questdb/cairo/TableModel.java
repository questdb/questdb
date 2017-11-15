package com.questdb.cairo;

import com.questdb.common.ColumnType;
import com.questdb.std.IntList;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.str.ImmutableCharSequence;
import com.questdb.std.str.Path;

import java.io.Closeable;

public class TableModel implements Closeable {
    private final String name;
    private final int partitionBy;
    private final AppendMemory mem = new AppendMemory();
    private final ObjList<CharSequence> columnNames = new ObjList<>();
    private final IntList columnTypes = new IntList();
    private final Path path = new Path();
    private final CairoConfiguration cairoCfg;
    private int timestampIndex = -1;

    public TableModel(CairoConfiguration cairoCfg, String name, int partitionBy) {
        this.cairoCfg = cairoCfg;
        this.name = name;
        this.partitionBy = partitionBy;
    }

    @Override
    public void close() {
        Misc.free(mem);
        Misc.free(path);
    }

    public TableModel col(CharSequence name, int type) {
        columnNames.add(ImmutableCharSequence.of(name));
        columnTypes.add(type);
        return this;
    }

    public CairoConfiguration getCairoCfg() {
        return cairoCfg;
    }

    public int getColumnCount() {
        return columnNames.size();
    }

    public ObjList<CharSequence> getColumnNames() {
        return columnNames;
    }

    public IntList getColumnTypes() {
        return columnTypes;
    }

    public AppendMemory getMem() {
        return mem;
    }

    public String getName() {
        return name;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public Path getPath() {
        return path;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public TableModel timestamp() {
        assert timestampIndex == -1;
        timestampIndex = columnNames.size();
        col("timestamp", ColumnType.TIMESTAMP);
        return this;
    }
}
