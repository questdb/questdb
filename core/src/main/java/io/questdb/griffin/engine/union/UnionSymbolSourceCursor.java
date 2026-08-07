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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.RecordCursor;

interface UnionSymbolSourceCursor {
    int bindSymbolSourceTracker(SymbolSourceTracker tracker, int nextSourceIndex);

    void updateSymbolSource();

    class SymbolSourceTracker {
        private RecordCursor cursor;
        private int sourceIndex = -1;

        void clear() {
            cursor = null;
            sourceIndex = -1;
        }

        RecordCursor getCursor() {
            return cursor;
        }

        int getSourceIndex() {
            return sourceIndex;
        }

        void of(RecordCursor cursor, int sourceIndex) {
            this.cursor = cursor;
            this.sourceIndex = sourceIndex;
        }
    }
}
