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

package io.questdb.cairo.security;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.cairo.TableToken;

public class ReadOnlyCairoSecurityContext implements CairoSecurityContext {
    public static final ReadOnlyCairoSecurityContext INSTANCE = new ReadOnlyCairoSecurityContext();

    @Override
    public void authorizeTableWrite(TableToken tableToken) {
        throw CairoException.nonCritical().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableCreate(CharSequence tableName) {
        throw CairoException.nonCritical().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableDrop() {
        throw CairoException.nonCritical().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableRename(TableToken tableToken) {
        throw CairoException.nonCritical().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeTableLock() {
        throw CairoException.nonCritical().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeDatabaseSnapshot() {
        throw CairoException.nonCritical().put("Write permission denied").setCacheable(true);
    }

    @Override
    public void authorizeCopyExecute() {
        throw CairoException.nonCritical().put("Write permission denied").setCacheable(true);
    }
}
