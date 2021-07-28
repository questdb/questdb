/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cutlass.text.TextUtil;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;

public class StringAdapter extends AbstractTypeAdapter {

    private final DirectCharSink utf8Sink;

    public StringAdapter(DirectCharSink utf8Sink) {
        this.utf8Sink = utf8Sink;
    }

    @Override
    public int getType() {
        return ColumnType.STRING;
    }

    @Override
    public boolean probe(CharSequence text) {
        // anything can be string, we do not to call this method to assert this
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectByteCharSequence value) throws Exception {
        utf8Sink.clear();
        TextUtil.utf8DecodeEscConsecutiveQuotes(value.getLo(), value.getHi(), utf8Sink);
        row.putStr(column, utf8Sink);
    }
}
