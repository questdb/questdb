/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.time.DateFormatUtils;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class PerformanceTest extends AbstractCairoTest {

    private static final int TEST_DATA_SIZE = 1_000_000;
    private static final Log LOG = LogFactory.getLog(PerformanceTest.class);

    @Test
    public void testCairoPerformance() throws NumericException {

        int count = 10;
        long t = 0;
        long result;

        String[] symbols = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        try (TableModel model = new TableModel(configuration, "quote", PartitionBy.NONE)
                .timestamp()
                .col("sym", ColumnType.SYMBOL)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL).symbolCapacity(2)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(2)) {
            CairoTestUtils.create(model);
        }
        try (TableWriter w = new TableWriter(configuration, "quote")) {
            for (int i = -count; i < count; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                w.truncate();
                long timestamp = DateFormatUtils.parseDateTime("2013-10-05T10:00:00.000Z");
                Rnd r = new Rnd();
                int n = symbols.length - 1;
                for (int i1 = 0; i1 < TEST_DATA_SIZE; i1++) {
                    TableWriter.Row row = w.newRow(timestamp);
                    row.putSym(1, symbols[Math.abs(r.nextInt() % n)]);
                    row.putDouble(2, Math.abs(r.nextDouble()));
                    row.putDouble(3, Math.abs(r.nextDouble()));
                    row.putInt(4, Math.abs(r.nextInt()));
                    row.putInt(5, Math.abs(r.nextInt()));
                    row.putSym(6, "LXE");
                    row.putSym(7, "Fast trading");
                    row.append();
                    timestamp += 1000;
                }
                w.commit();
            }
            result = System.nanoTime() - t;
        }

        LOG.info().$("Cairo append (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result / count)).$("ms").$();

        try (TableReader reader = new TableReader(configuration, "quote")) {
            for (int i = -count; i < count; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }

                RecordCursor cursor = reader.getCursor();
                Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    record.getDate(0);
                    record.getSym(1);
                    record.getDouble(2);
                    record.getDouble(3);
                    record.getInt(4);
                    record.getInt(5);
                    record.getSym(6);
                    record.getSym(7);
                }
            }
            result = (System.nanoTime() - t) / count;
        }
        LOG.info().$("Cairo read (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result)).$("ms").$();
    }
}
