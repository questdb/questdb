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

import io.questdb.cairo.TableReader;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A cursor for navigating through partition frames.
 */
public interface PartitionFrameCursor extends QuietCloseable, SymbolTableSource {

    default void calculateSize(RecordCursor.Counter counter) {
    }

    /**
     * Returns the table reader. The same TableReader is available on each partition frame.
     *
     * @return the table reader
     */
    TableReader getTableReader();

    /**
     * @return the next element in the partition frame
     */
    default @Nullable PartitionFrame next() {
        return next(0);
    }

    @Nullable PartitionFrame next(long skipTarget);

    /**
     * Reload the partition frame and return the cursor to the beginning of
     * the partition frame
     *
     * @return true when reload data has changed, false otherwise
     */
    @TestOnly
    boolean reload();

    /**
     * @return number of rows in all partition frames.
     */
    long size();

    /**
     * @return true if cursor supports fast size calculation,
     * i.e. {@link #calculateSize(RecordCursor.Counter)} is properly implemented.
     */
    default boolean supportsSizeCalculation() {
        return false;
    }

    /**
     * Return the cursor to the first partition frame.
     */
    void toTop();
}
