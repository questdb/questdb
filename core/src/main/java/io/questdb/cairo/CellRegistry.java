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

package io.questdb.cairo;

import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.StringSink;

/**
 * Thin wrapper over the {@code _cell} symbol map ({@link CompositeInternerLayout#REGISTRY_NAME}):
 * the write side interns composite-partition dimension-tuples ({@code int[]}) into dense cell
 * ordinals, the read side reverse-looks-up an ordinal back to its tuple.
 * <p>
 * A {@code CellRegistry} is opened around <b>either</b> a {@link MapWriter} (write side) <b>or</b> a
 * {@link SymbolMapReader} (read side), never both -- {@link #internCell(int[], int)} requires a
 * writer and {@link #getTuple(int, int[])} requires a reader. Reverse lookup is inherently a
 * read-side operation: {@link SymbolMapWriter} exposes no {@code valueOf(int)}.
 */
public class CellRegistry implements QuietCloseable {
    private final StringSink sink = new StringSink();
    private SymbolMapReader reader;
    private MapWriter writer;

    public CellRegistry(MapWriter writer) {
        this.writer = writer;
    }

    public CellRegistry(SymbolMapReader reader) {
        this.reader = reader;
    }

    @Override
    public void close() {
        writer = Misc.freeIfCloseable(writer);
        reader = Misc.freeIfCloseable(reader);
    }

    /**
     * Reverse-looks-up dense cell ordinal {@code ordinal} back to its dimension-tuple, decoding it
     * into {@code tupleOut}. Read-side only.
     *
     * @throws IllegalStateException if this registry was opened over a writer, not a reader
     */
    public void getTuple(int ordinal, int[] tupleOut) {
        if (reader == null) {
            throw new IllegalStateException("CellRegistry opened write-only");
        }
        CompositeTupleCodec.decode(reader.valueOf(ordinal), tupleOut);
    }

    /**
     * Interns dimension-tuple {@code tuple[0, arity)} into a dense cell ordinal: repeated calls
     * with an equal tuple return the same ordinal (dedup), stable across the life of the
     * underlying symbol map. Write-side only.
     *
     * @throws IllegalStateException if this registry was opened over a reader, not a writer
     */
    public int internCell(int[] tuple, int arity) {
        if (writer == null) {
            throw new IllegalStateException("CellRegistry opened read-only");
        }
        sink.clear();
        CompositeTupleCodec.encode(tuple, arity, sink);
        return writer.put(sink);
    }

    public int size() {
        return writer != null ? writer.getSymbolCount() : reader.getSymbolCount();
    }
}
