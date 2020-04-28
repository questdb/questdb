/* ******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.OnePageMemory;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.*;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.time.DateFormatUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.TEMP_PARTITION_NAME;
import static org.junit.Assert.assertEquals;

public class InsertOutOfOrderTest extends AbstractGriffinTest {

    private final static char ENTRY_DELIMITER = '\t';
    private final static long MICROS_IN_A_DAY = 86_400_000_000L;
    private final static long MICROS_IN_HALF_A_DAY = MICROS_IN_A_DAY / 2;

    @Before
    public void setup() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testInsertOutOfOrderWhenLastRowMatchesMaxTimestamp() throws Exception {

        final String expected = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-02T00:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-03T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-04T00:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-05T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-06T00:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-07T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-08T00:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-09T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-10T00:00:00.000000Z\n";
        assertQuery(expected,
                "select * from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(0, " + MICROS_IN_A_DAY + ") k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by DAY",
                "k");

        final String expectedAfterInsert = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-02T00:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-03T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-04T00:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-05T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-06T00:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-07T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-08T00:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-09T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-10T00:00:00.000000Z\n";

        assertQuery(expectedAfterInsert,
                "select * from x",
                "insert into x select * from " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(to_timestamp('1970-01-09T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), " + MICROS_IN_A_DAY + ") k" +
                        " from" +
                        " long_sequence(2)" +
                        ") timestamp(k) partition by DAY",
                "k");

        String expectedTempColumns = "24.59345277606021\t49.00510449885239\n" +
                "RFBV\tOOZZ\n" +
                "1970-01-09T00:00:00.000000Z\t1970-01-10T00:00:00.000000Z";

        assertContentsOfTempRows(expectedTempColumns, "x");
    }

    @Test
    public void testInsertOutOfOrderToFirstPartition() throws Exception {

        final String expected = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-02T00:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-03T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-04T00:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-05T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-06T00:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-07T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-08T00:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-09T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-10T00:00:00.000000Z\n";
        assertQuery(expected,
                "select * from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(0, " + MICROS_IN_A_DAY + ") k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by DAY",
                "k");

        final String expectedAfterInsert = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-02T00:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-03T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-04T00:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-05T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-06T00:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-07T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-08T00:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-09T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-10T00:00:00.000000Z\n";

        assertQuery(expectedAfterInsert,
                "select * from x",
                "insert into x select * from " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(to_timestamp('1970-01-01T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), " + MICROS_IN_A_DAY + ") k" +
                        " from" +
                        " long_sequence(2)" +
                        ") timestamp(k) partition by DAY",
                "k");

        String expectedTempColumns = "24.59345277606021\t49.00510449885239\n" +
                "RFBV\tOOZZ\n" +
                "1970-01-01T00:00:00.000000Z\t1970-01-02T00:00:00.000000Z";

        assertContentsOfTempRows(expectedTempColumns, "x");
    }

    @Test
    public void testInsertOutOfOrderToLastPartition() throws Exception {

        final String expected = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-01T12:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-02T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-02T12:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-03T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-03T12:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-04T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-04T12:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-05T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-05T12:00:00.000000Z\n";
        assertQuery(expected,
                "select * from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(0, " + MICROS_IN_HALF_A_DAY + ") k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by DAY",
                "k");

        final String expectedAfterInsert = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-01T12:00:00.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-02T00:00:00.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-02T12:00:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-03T00:00:00.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-03T12:00:00.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-04T00:00:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-04T12:00:00.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-05T00:00:00.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-05T12:00:00.000000Z\n";

        assertQuery(expectedAfterInsert,
                "select * from x",
                "insert into x select * from " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(to_timestamp('1970-01-05T01:00:00', 'yyyy-MM-ddTHH:mm:ss'), " + MICROS_IN_HALF_A_DAY + ") k" +
                        " from" +
                        " long_sequence(2)" +
                        ") timestamp(k) partition by DAY",
                "k");

        String expectedTempColumns = "24.59345277606021\t49.00510449885239\n" +
                "RFBV\tOOZZ\n" +
                "1970-01-05T01:00:00.000000Z\t1970-01-05T13:00:00.000000Z";

        assertContentsOfTempRows(expectedTempColumns, "x");
    }

    @Test
    public void testInsertOutOfOrderToLastPartitionMoreColumnTypes() throws Exception {

        final String expected = "a1\ta\tb\tc\td\te\tf\tf1\tg\ti\tj\tj1\tl\tm\tk\n" +
                "1569490116\tNaN\tfalse\t\tNaN\t0.7611\t428\t-1593\t2015-04-04T16:34:47.226Z\t\t190\t4086802474270249591\t27\t00000000 26 af 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1\t1970-01-01T00:00:00.000000Z\n" +
                "-938514914\t21\ttrue\tBEO\t0.5793466326862211\t0.9687\t813\t15926\t2015-05-11T16:54:52.050Z\tVTJW\t148\t-6794405451419334859\t6\t\t1970-01-01T12:00:00.000000Z\n" +
                "-1162267908\t21\tfalse\tRSZ\t0.7763904674818695\t0.9750\t571\t-3567\t2015-02-19T07:07:43.737Z\tVTJW\tNaN\t-7316123607359392486\t26\t00000000 40 e2 4b b1 3e e3 f1 f1 1e ca 9c\t1970-01-02T00:00:00.000000Z\n" +
                "1196016669\t0\ttrue\tKGH\t0.45659895188239796\t0.9566\t230\t-19773\t2015-08-01T16:34:47.067Z\tPEHN\t106\t-4908948886680892316\t16\t00000000 d7 6f b8 c9 ae 28 c7 84 47 dc\t1970-01-02T12:00:00.000000Z\n" +
                "-1389094446\t3\ttrue\tGSH\t0.44804689668613573\t0.3495\t869\t-1072\t\tCPSW\tNaN\t-7256514778130150964\t6\t00000000 bb 64 d2 ad 49 1c f2 3c ed 39 ac a8 3b a6\t1970-01-03T00:00:00.000000Z\n";
        assertQuery(expected,
                "select * from allTypes",
                "create table allTypes as " +
                        "(" +
                        "select" +
                        " rnd_int() a1," +
                        " rnd_int(0, 30, 2) a," +
                        " rnd_boolean() b," +
                        " rnd_str(3,3,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_short() f1," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long(100,200,2) j," +
                        " rnd_long() j1," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " timestamp_sequence(0, " + MICROS_IN_HALF_A_DAY + ") k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                "k");

        final String expectedAfterInsert = "a1\ta\tb\tc\td\te\tf\tf1\tg\ti\tj\tj1\tl\tm\tk\n" +
                "1569490116\tNaN\tfalse\t\tNaN\t0.7611\t428\t-1593\t2015-04-04T16:34:47.226Z\t\t190\t4086802474270249591\t27\t00000000 26 af 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1\t1970-01-01T00:00:00.000000Z\n" +
                "-938514914\t21\ttrue\tBEO\t0.5793466326862211\t0.9687\t813\t15926\t2015-05-11T16:54:52.050Z\tVTJW\t148\t-6794405451419334859\t6\t\t1970-01-01T12:00:00.000000Z\n" +
                "-1162267908\t21\tfalse\tRSZ\t0.7763904674818695\t0.9750\t571\t-3567\t2015-02-19T07:07:43.737Z\tVTJW\tNaN\t-7316123607359392486\t26\t00000000 40 e2 4b b1 3e e3 f1 f1 1e ca 9c\t1970-01-02T00:00:00.000000Z\n" +
                "1196016669\t0\ttrue\tKGH\t0.45659895188239796\t0.9566\t230\t-19773\t2015-08-01T16:34:47.067Z\tPEHN\t106\t-4908948886680892316\t16\t00000000 d7 6f b8 c9 ae 28 c7 84 47 dc\t1970-01-02T12:00:00.000000Z\n" +
                "-1389094446\t3\ttrue\tGSH\t0.44804689668613573\t0.3495\t869\t-1072\t\tCPSW\tNaN\t-7256514778130150964\t6\t00000000 bb 64 d2 ad 49 1c f2 3c ed 39 ac a8 3b a6\t1970-01-03T00:00:00.000000Z\n";

        assertQuery(expectedAfterInsert,
                "select * from allTypes",
                "insert into allTypes select * from " +
                        "(" +
                        "select" +
                        " rnd_int() a1," +
                        " rnd_int(0, 30, 2) a," +
                        " rnd_boolean() b," +
                        " rnd_str(3,3,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_short() f1," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long(100,200,2) j," +
                        " rnd_long() j1," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " timestamp_sequence(0, " + MICROS_IN_HALF_A_DAY + ") k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                "k");
    }

    private void assertContentsOfTempRows(String expected, String tableName) {
        TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
        RecordMetadata metadata = reader.getMetadata();

        Path path = getPath(tableName);

        int columnCount = metadata.getColumnCount();
        String[] expectedColumns = expected.split("\n");
        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);

            String columnName = metadata.getColumnName(i);
            OnePageMemory mem = mapMemory(columnName, path);

            String expectedColumn = expectedColumns[i];
            String[] expectedColumnEntries = expectedColumn.split("\t");
            int entryCount = expectedColumnEntries.length;

            String actualColumn = getActualColumn(columnType, mem, entryCount);
            assertEquals(expectedColumn, actualColumn);

        }
        reader.close();
    }

    @NotNull
    private OnePageMemory mapMemory(CharSequence columnName, Path path) {
        int rootLength = path.length();
        path.put(columnName).put(".d");
        FilesFacade ff = configuration.getFilesFacade();
        OnePageMemory mem = new OnePageMemory(ff, path, ff.length(path));
        path.trimTo(rootLength);
        return mem;
    }

    @NotNull
    private Path getPath(String tableName) {
        CharSequence root = configuration.getRoot();
        Path path = new Path();
        path.of(root);
        path.concat(tableName).put(Files.SEPARATOR).put(TEMP_PARTITION_NAME).put(Files.SEPARATOR);
        return path;
    }

    private String getActualColumn(int columnType, OnePageMemory mem, int entryCount) {
        sink.clear();
        long offset = 0;
        for (int j = 0; j < entryCount; j++) {
            if (j > 0) sink.put(ENTRY_DELIMITER);
            offset = printEntry(mem, columnType, offset);

        }
        return sink.toString();
    }


    private long printEntry(OnePageMemory mem, int columnType, long offset) {
        switch (columnType) {
            case ColumnType.DATE:
                DateFormatUtils.appendDateTime(sink, mem.getLong(offset));
                return offset + 8;
            case ColumnType.TIMESTAMP:
                TimestampFormatUtils.appendDateTimeUSec(sink, mem.getLong(offset));
                return offset + 8;
            case ColumnType.LONG:
                sink.put(mem.getLong(offset));
                return offset + 8;
            case ColumnType.DOUBLE:
                sink.put(mem.getDouble(offset));
                return offset + 8;
            case ColumnType.FLOAT:
                sink.put(mem.getFloat(offset), 4);
                return offset + 4;
            case ColumnType.INT:
                sink.put(mem.getInt(offset));
                return offset + 4;
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                CharSequence str = mem.getStr(offset);
                sink.put(str);
                return offset + 4 + str.length() * 2;
            case ColumnType.SHORT:
                sink.put(mem.getShort(offset));
                return offset + 2;
            case ColumnType.CHAR:
                char c = mem.getChar(offset);
                if (c > 0) {
                    sink.put(c);
                }
                return offset + 2;
            case ColumnType.BYTE:
                sink.put(mem.getByte(offset));
                return offset + 1;
            case ColumnType.BOOLEAN:
                sink.put(mem.getBool(offset));
                return offset + 1;
            case ColumnType.BINARY:
                BinarySequence bin = mem.getBin(offset);
                Chars.toSink(bin, sink);
                return offset + bin.length();
            //TODO
//            case ColumnType.LONG256:
//                mem.getLong256(i, sink);
//                break;
            default:
                break;
        }
        return offset;
    }
}
