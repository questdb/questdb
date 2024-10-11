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


import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;

public interface CairoMetadataRO extends AutoCloseable, QuietCloseable {
    void filterVisibleTables(HashMap<CharSequence, CairoTable> localCache);

    @Nullable
    CairoTable getTable(@NotNull TableToken tableToken);

    int getTableCount();

    long getVersion();

    @Nullable
    CairoTable getVisibleTable(@NotNull TableToken tableToken);

    void snapshotCreate(HashMap<CharSequence, CairoTable> localCache);

    long snapshotRefresh(HashMap<CharSequence, CairoTable> localCache, long priorVersion);

    String toString0();

}
