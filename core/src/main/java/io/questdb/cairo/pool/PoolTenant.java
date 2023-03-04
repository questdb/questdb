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

package io.questdb.cairo.pool;

import io.questdb.cairo.TableToken;
import io.questdb.std.QuietCloseable;

public interface PoolTenant extends QuietCloseable {

    /**
     * Pool tenant must keep track of the Entry it belongs to and provide this entry when requested. Entry is
     * usually assigned to the tenant in the constructor.
     *
     * @param <T> typically type of the subclass
     * @return entry instance.
     */
    <T> AbstractMultiTenantPool.Entry<T> getEntry();

    /**
     * Opaque index, which is usually assigned to tenant in the constructor. Tenant instances must keep it safe and
     * unchanged and provide when requested.
     *
     * @return opaque index value
     */
    int getIndex();

    /**
     * Name of table this reader is attached to. Pooled reader instances cannot be reused across
     * more than one table.
     *
     * @return valid table name.
     */
    TableToken getTableToken();

    /**
     * Pool informs the reader that it no longer belongs to the pool. Implementations must assume that
     * subsequent calls to close() method can no longer be delegated to the pool.
     */
    void goodbye();

    /**
     * Pool informs the reader that the instance is being returned to the new owner and the new owner expects
     * the reader to be fully up-to-date with all data and metadata changes.
     */
    void refresh();

    /**
     * Refreshes value of the Table Token to the one it was created with.
     *
     * @param tableToken new value of the Table Token to update to
     */
    void updateTableToken(TableToken tableToken);
}
