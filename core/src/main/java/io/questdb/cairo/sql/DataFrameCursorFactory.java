/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;

import java.io.Closeable;

/**
 * A factory interface for dataframe cursors
 */
public interface DataFrameCursorFactory extends Sinkable, Closeable, Plannable {

    // Any order means that algorithm is able to work with frames in any order.
    // In this case frame order will be driven by the optimiser.
    // Any order is not returned by the factory
    int ORDER_ANY = 2;
    int ORDER_ASC = 0;
    int ORDER_DESC = 1;

    static CharSequence nameOf(int order) {
        switch (order) {
            case ORDER_ASC:
                return "forward";
            case ORDER_DESC:
                return "backward";
            default:
                return "any";
        }
    }

    static int reverse(int order) {
        switch (order) {
            case ORDER_ASC:
                return ORDER_DESC;
            case ORDER_DESC:
                return ORDER_ASC;
            default:
                return ORDER_ANY;
        }
    }

    @Override
    void close();

    DataFrameCursor getCursor(SqlExecutionContext executionContext, int order) throws SqlException;

    RecordMetadata getMetadata();

    /**
     * Order of records in the data frame in regard to timestamp.
     *
     * @return 0 for ascending and 1 for descending
     */
    int getOrder();

    boolean supportTableRowId(TableToken tableToken);

    /**
     * @param sink to print data frame cursor to
     */
    default void toSink(CharSink sink) {
        throw new UnsupportedOperationException();
    }
}
