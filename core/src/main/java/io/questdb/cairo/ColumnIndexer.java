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

package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;


public interface ColumnIndexer extends QuietCloseable {

    void closeSlider();

    void configureFollowerAndWriter(
            Path path,
            CharSequence name,
            long columnNameTxn,
            MemoryMA columnMem,
            long columnTop
    );

    void configureWriter(Path path, CharSequence name, long columnNameTxn, long columnTop);

    void distress();

    int getFd();

    long getSequence();

    BitmapIndexWriter getWriter();

    void index(MemoryR mem, long loRow, long hiRow);

    boolean isDistressed();

    void refreshSourceAndIndex(long loRow, long hiRow);

    void rollback(long maxRow);

    boolean tryLock(long expectedSequence);

    void sync(boolean async);
}
