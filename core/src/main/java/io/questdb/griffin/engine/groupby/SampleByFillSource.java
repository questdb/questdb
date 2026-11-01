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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.RecordCursor;

/**
 * A SAMPLE BY cursor that a SAMPLE BY fill reads without re-reading or copying its
 * rows. {@link #peekNextTimestamp()} reports the timestamp of the next row before the
 * cursor computes it, so the current row stays readable while the fill emits the gap
 * rows that precede the next one. Before its first row, the current row holds NULL
 * values.
 */
interface SampleByFillSource extends RecordCursor {

    /**
     * Returns the timestamp of the row the next {@link #hasNext()} returns, or
     * LONG_NULL when no row is left.
     */
    long peekNextTimestamp();

    /**
     * Makes the current row report the given timestamp until the cursor computes its
     * next row, so a gap row can be the current row itself.
     */
    void setGapTimestamp(long timestamp);
}
