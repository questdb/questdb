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

package io.questdb.cutlass.line.tcp;

import io.questdb.mp.Job;
import io.questdb.std.Pool;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8String;

public interface NetworkIOJob extends Job, QuietCloseable {
    void addTableUpdateDetails(Utf8String tableNameUtf8, TableUpdateDetails tableUpdateDetails);

    TableUpdateDetails getLocalTableDetails(DirectUtf8Sequence tableNameUtf8);

    Pool<SymbolCache> getSymbolCachePool();

    int getWorkerId();

    void releaseWalTableDetails();
}
