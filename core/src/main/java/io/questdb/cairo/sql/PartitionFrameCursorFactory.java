/*******************************************************************************
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

package io.questdb.cairo.sql;

import io.questdb.cairo.TableToken;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * A factory interface for partition frame cursors.
 */
public interface PartitionFrameCursorFactory extends Sinkable, Closeable, Plannable {

    // Any order means that algorithm is able to work with frames in any order.
    // In this case frame order will be driven by the optimiser.
    // Any order is not returned by the factory
    int ORDER_ANY = 2;
    int ORDER_ASC = 0;
    int ORDER_DESC = 1;

    static CharSequence nameOf(int order) {
        return switch (order) {
            case ORDER_ASC -> "forward";
            case ORDER_DESC -> "backward";
            default -> "any";
        };
    }

    static int reverse(int order) {
        return switch (order) {
            case ORDER_ASC -> ORDER_DESC;
            case ORDER_DESC -> ORDER_ASC;
            default -> ORDER_ANY;
        };
    }

    @Override
    void close();

    PartitionFrameCursor getCursor(SqlExecutionContext executionContext, IntList columnIndexes, int order) throws SqlException;

    RecordMetadata getMetadata();

    /**
     * Order of records in the partition frame in regard to timestamp.
     *
     * @return 0 for ascending and 1 for descending
     */
    int getOrder();

    TableToken getTableToken();

    boolean supportsTableRowId(TableToken tableToken);

    /**
     * @param sink to print partition frame cursor to
     */
    default void toSink(@NotNull CharSink<?> sink) {
        throw new UnsupportedOperationException();
    }
}
