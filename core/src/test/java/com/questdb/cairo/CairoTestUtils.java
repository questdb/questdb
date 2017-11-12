package com.questdb.cairo;

import com.questdb.misc.FilesFacade;
import com.questdb.misc.FilesFacadeImpl;
import com.questdb.std.str.Path;
import com.questdb.std.time.Dates;
import com.questdb.store.PartitionBy;
import com.questdb.store.factory.configuration.JournalStructure;

public class CairoTestUtils {

    public static void createAllTable(CharSequence root, int partitionBy) {
        createTable(FilesFacadeImpl.INSTANCE, root, getAllStructure().partitionBy(partitionBy));
    }

    public static String createTable(FilesFacade ff, CharSequence root, JournalStructure struct) {
        String name = struct.getName();
        try (AppendMemory mem = new AppendMemory()) {
            try (Path path = new Path()) {
                if (TableUtils.exists(ff, path, root, name) == TableUtils.TABLE_DOES_NOT_EXIST) {
                    TableUtils.create(ff, path, mem, root, struct.build(), 509);
                } else {
                    throw CairoException.instance(0).put("Table ").put(name).put(" already exists");
                }
            }
        }
        return name;
    }

    public static JournalStructure getAllStructure() {
        return new JournalStructure("all").
                $int("int").
                $short("short").
                $byte("byte").
                $double("double").
                $float("float").
                $long("long").
                $str("str").
                $sym("sym").
                $bool("bool").
                $bin("bin").
                $date("date");
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
