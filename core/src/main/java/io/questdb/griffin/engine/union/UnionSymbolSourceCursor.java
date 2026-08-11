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
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.functions.SymbolFunction;
import org.jetbrains.annotations.Nullable;

interface UnionSymbolSourceCursor {
    int bindSymbolSourceTracker(SymbolSourceTracker tracker, int nextSourceIndex);

    // One native-key leaf is enough to make the result key path worthwhile: that leaf avoids text
    // work while dynamic leaves continue to mint merged keys from their values.
    boolean hasKeyValueSymbolTable(int columnIndex);

    void updateSymbolSource();

    static boolean hasKeyValueSymbolTable(RecordCursor cursor, int columnIndex) {
        return cursor instanceof UnionSymbolSourceCursor sourceCursor
                ? sourceCursor.hasKeyValueSymbolTable(columnIndex)
                : keyValueSymbolTable(cursor, columnIndex) != null;
    }

    @Nullable
    static SymbolTable keyValueSymbolTable(RecordCursor cursor, int columnIndex) {
        try {
            SymbolTable symbolTable = cursor.getSymbolTable(columnIndex);
            if (symbolTable instanceof SymbolFunction symbolFunction) {
                final StaticSymbolTable staticSymbolTable = symbolFunction.getStaticSymbolTable();
                if (staticSymbolTable != null) {
                    symbolTable = staticSymbolTable;
                }
            }
            return symbolTable != null && symbolTable.supportsKeyValueAccess() ? symbolTable : null;
        } catch (UnsupportedOperationException ignored) {
            // Dynamic expressions and cursors without symbol tables use text fallback.
            return null;
        }
    }

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
