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

package io.questdb.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.network.SuspendEvent;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

public interface WalTxnSuspendEvents extends QuietCloseable {

    /**
     * Returns suspend event in case if the given WAL transaction is still not visible for readers
     * or {@code null} if it's already visible.
     *
     * @param tableToken WAL table token
     * @param txn        transaction number
     * @return suspend event to be used in {@link io.questdb.network.IODispatcher} or {@code null}.
     */
    @Nullable SuspendEvent register(TableToken tableToken, long txn);

    /**
     * Appends current registered suspend events to the provided list.
     *
     * @param tableToken WAL table token
     * @param dest       destination list; this method doesn't clear the list
     */
    void takeRegisteredEvents(TableToken tableToken, ObjList<SuspendEvent> dest);
}
