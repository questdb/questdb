package com.questdb.cairo;

import com.questdb.misc.Files;
import com.questdb.misc.FilesFacade;
import com.questdb.std.str.Path;
import com.questdb.std.time.Dates;
import com.questdb.store.ColumnType;
import com.questdb.store.PartitionBy;

import static com.questdb.cairo.TableUtils.META_FILE_NAME;
import static com.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class CairoTestUtils {

    public static void create(TableModel model) {
        final Path path = model.getPath();
        final FilesFacade ff = model.getCairoCfg().getFilesFacade();

        path.of(model.getCairoCfg().getRoot()).concat(model.getName());
        final int rootLen = path.length();
        if (ff.mkdirs(path.put(Files.SEPARATOR).$(), model.getCairoCfg().getMkDirMode()) == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot create dir: ").put(path);
        }

        try (AppendMemory mem = model.getMem()) {

            mem.of(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());

            int count = model.getColumnCount();
            mem.putInt(count);
            mem.putInt(model.getPartitionBy());
            mem.putInt(model.getTimestampIndex());
            for (int i = 0; i < count; i++) {
                mem.putInt(model.getColumnTypes().getQuick(i));
            }
            for (int i = 0; i < count; i++) {
                mem.putStr(model.getColumnNames().getQuick(i));
            }

            mem.of(ff, path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), ff.getPageSize());
            TableUtils.resetTxn(mem);
        }
    }

    public static void createAllTable(CairoConfiguration configuration, int partitionBy) {
        try (TableModel model = getAllTypesModel(configuration, partitionBy)) {
            create(model);
        }
    }

    public static TableModel getAllTypesModel(CairoConfiguration configuration, int partitionBy) {
        return new TableModel(configuration, "all", partitionBy)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE);
    }

    static boolean isSamePartition(long timestampA, long timestampB, int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.NONE:
                return true;
            case PartitionBy.DAY:
                return Dates.floorDD(timestampA) == Dates.floorDD(timestampB);
            case PartitionBy.MONTH:
                return Dates.floorMM(timestampA) == Dates.floorMM(timestampB);
            case PartitionBy.YEAR:
                return Dates.floorYYYY(timestampA) == Dates.floorYYYY(timestampB);
            default:
                throw CairoException.instance(0).put("Cannot compare timestamps for unsupported partition type: [").put(partitionBy).put(']');
        }
    }
}
