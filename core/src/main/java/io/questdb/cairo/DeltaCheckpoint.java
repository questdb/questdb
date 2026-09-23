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

package io.questdb.cairo;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.FilesFacade;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;

/** Captures reader-visible Delta catalogs and retains their files until close. */
public interface DeltaCheckpoint extends QuietCloseable {
    String CATALOG_SUFFIX = "_delta";
    String DIRECTORY_NAME = "_delta";
    DeltaCheckpoint UNSUPPORTED = (reader, checkpoint, circuitBreaker) -> {
        if (reader.hasAnyDelta()) {
            throw CairoException.nonCritical().put("Delta checkpoint capture is not supported");
        }
    };

    void capture(TableReader reader, Path checkpoint, SqlExecutionCircuitBreaker circuitBreaker);

    /** Releases all captures, including partial captures. The adapter can then be reused. */
    @Override
    default void close() {
    }

    /** Validates and installs captured catalogs, then removes later Delta state before table repair. */
    default void restore(FilesFacade ff, Path checkpoint, Path table) {
        final int tableLen = table.size();
        try {
            if (ff.exists(table.concat(DIRECTORY_NAME).$())) {
                throw CairoException.nonCritical().put("Delta checkpoint recovery is not supported");
            }
            final DirectUtf8StringZ nameSink = new DirectUtf8StringZ();
            ff.iterateDir(checkpoint.$(), (name, type) -> {
                if (Utf8s.endsWithAscii(nameSink.of(name), CATALOG_SUFFIX)) {
                    throw CairoException.nonCritical().put("Delta checkpoint recovery is not supported");
                }
            });
        } finally {
            table.trimTo(tableLen);
        }
    }

    /** Releases retained catalogs before recovery replaces their files. Called before tables open. */
    default void startRestore() {
    }
}
