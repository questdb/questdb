/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.network.SuspendEvent;
import io.questdb.std.ThreadLocal;

/**
 * Thrown when the queried data is in a cold partition and a request to download it
 * to a local disk has been started. The querying side should switch to other tasks
 * and retry {@link RecordCursor#hasNext()} call later when the data is moved to
 * the local disk.
 */
public class DataUnavailableException extends CairoException {
    private static final ThreadLocal<DataUnavailableException> tlException = new ThreadLocal<>(DataUnavailableException::new);

    private SuspendEvent event;

    public static DataUnavailableException instance(TableToken tableToken, CharSequence partition, SuspendEvent event) {
        DataUnavailableException ex = tlException.get();
        ex.message.clear();
        ex.errno = CairoException.NON_CRITICAL;
        ex.event = event;
        ex.put("partition is located in cold storage, query will be suspended [table=").put(tableToken.getTableName())
                .put(", partition=").put(partition)
                .put(']');
        return ex;
    }

    public SuspendEvent getEvent() {
        return event;
    }
}
