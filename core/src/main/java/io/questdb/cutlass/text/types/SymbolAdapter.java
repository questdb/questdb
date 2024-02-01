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

package io.questdb.cutlass.text.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cutlass.text.Utf8Exception;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8s;

public class SymbolAdapter extends AbstractTypeAdapter {

    private final boolean indexed;
    private final DirectUtf16Sink utf16Sink;

    public SymbolAdapter(DirectUtf16Sink utf16Sink, boolean indexed) {
        this.utf16Sink = utf16Sink;
        this.indexed = indexed;
    }

    @Override
    public int getType() {
        return ColumnType.SYMBOL;
    }

    @Override
    public boolean isIndexed() {
        return indexed;
    }

    @Override
    public boolean probe(DirectUtf8Sequence text) {
        // any text can be a symbol if we look at it without context
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value) throws Exception {
        write(row, column, value, utf16Sink);
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value, DirectUtf16Sink utf16Sink) throws Exception {
        utf16Sink.clear();
        if (!Utf8s.utf8ToUtf16EscConsecutiveQuotes(value.lo(), value.hi(), utf16Sink)) {
            throw Utf8Exception.INSTANCE;
        }
        row.putSym(column, utf16Sink);
    }
}
