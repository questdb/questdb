/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.LongList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;

public interface FuzzTransactionOperation extends QuietCloseable {

    /**
     * Formats a SQL WHERE clause that selects partitions in the day before
     * <code>cutoffTimestamp</code>, the timestamp being in the table's native unit.
     */
    static String partitionDayBeforeFilter(RecordMetadata metadata, long cutoffTimestamp) {
        final TimestampDriver driver = ColumnType.getTimestampDriver(metadata.getTimestampType());
        final String tsColumnName = metadata.getColumnName(metadata.getTimestampIndex());
        final StringSink sink = new StringSink();
        sink.put(tsColumnName).put(" > '");
        driver.append(sink, cutoffTimestamp - driver.fromDays(1));
        sink.put("' AND ").put(tsColumnName).put(" < '");
        driver.append(sink, cutoffTimestamp);
        sink.put('\'');
        return sink.toString();
    }

    boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI tableWriter, int virtualTimestampIndex, LongList excludedTsIntervals);

    default void close() {
    }
}
