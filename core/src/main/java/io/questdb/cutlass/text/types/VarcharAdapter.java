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

package io.questdb.cutlass.text.types;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Decimal256;
import io.questdb.std.SwarUtils;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;


public class VarcharAdapter extends AbstractTypeAdapter {
    private static final byte DOUBLE_QUOTE = '"';
    private static final long DOUBLE_QUOTE_WORD = SwarUtils.broadcast(DOUBLE_QUOTE);
    private final DirectUtf8Sink utf8Sink;

    public VarcharAdapter(DirectUtf8Sink utf8Sink) {
        this.utf8Sink = utf8Sink;
    }

    @Override
    public int getType() {
        return ColumnType.VARCHAR;
    }

    @Override
    public boolean probe(DirectUtf8Sequence text) {
        // anything can be string, we do not to call this method to assert this
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value) throws Exception {
        write(row, column, value, null, utf8Sink, null);
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value, DirectUtf16Sink utf16Sink, DirectUtf8Sink utf8Sink, Decimal256 decimal256) {
        deflateConsecutiveDoubleQuotes(value, utf8Sink);
        row.putVarchar(column, utf8Sink);
    }

    // replacing consecutive double quotes with a single one
    private static void deflateConsecutiveDoubleQuotes(DirectUtf8Sequence value, DirectUtf8Sink utf8Sink) {
        utf8Sink.clear();
        int quoteCount = 0;
        final int len = value.size();
        int i = 0;
        while (i < len) {
            if (i < len - 7) {
                final long word = value.longAt(i);
                final long zeroBytesWord = SwarUtils.markZeroBytes(word ^ DOUBLE_QUOTE_WORD);
                if (zeroBytesWord == 0) {
                    // fast path for no double quotes in consequent 8 bytes
                    quoteCount = 0;
                    utf8Sink.putAny8(word);
                    i += 8;
                    continue;
                }
            }

            byte b = value.byteAt(i++);
            if (b == DOUBLE_QUOTE) {
                if (quoteCount++ % 2 == 0) {
                    utf8Sink.putAny(DOUBLE_QUOTE);
                }
            } else {
                quoteCount = 0;
                utf8Sink.putAny(b);
            }
        }
        if (quoteCount % 2 != 0) {
            utf8Sink.putAny(DOUBLE_QUOTE);
        }
    }
}
