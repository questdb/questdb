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

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.StampedLock;

public class CairoTable {
    private TableToken token;
    private StampedLock tokenLock;

    public CairoTable(@NotNull TableToken token) {
        this.tokenLock = new StampedLock();
        this.token = token;
    }

    /**
     * Get table name thread safe.
     *
     * @param tableName
     * @return
     */
    public @NotNull String getName(@NotNull CharSequence tableName) {
        // try an optimistic read
        long optimisticStamp = tokenLock.tryOptimisticRead();
        String name = token.getTableName();
        // check the read
        if (!tokenLock.validate(optimisticStamp)) {
            // upgrade the lock
            final long upgradedStamp = tokenLock.readLock();
            name = token.getTableName();
            tokenLock.unlockRead(upgradedStamp);
        }
        return name;
    }

    /**
     * Get table token, not thread safe.
     *
     * @return
     */
    private TableToken getTokenUnsafe() {
        return token;
    }

//
//    @NotNull
//    private final GcUtf8String dirName;
//    private final boolean isProtected;
//    private final boolean isPublic;
//    private final boolean isSystem;
//    private final boolean isWal;
//    private final int tableId;
//    @NotNull
//    private final String tableName;

}
