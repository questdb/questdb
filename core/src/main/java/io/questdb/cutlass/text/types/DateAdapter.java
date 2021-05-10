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
import io.questdb.std.Mutable;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.DirectByteCharSequence;

public class DateAdapter extends AbstractTypeAdapter implements Mutable, TimestampCompatibleAdapter {
    private DateLocale locale;
    private DateFormat format;

    @Override
    public void clear() {
        this.format = null;
        this.locale = null;
    }

    @Override
    public int getType() {
        return ColumnType.DATE;
    }

    @Override
    public boolean probe(CharSequence text) {
        try {
            format.parse(text, locale);
            return true;
        } catch (NumericException e) {
            return false;
        }
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectByteCharSequence value) throws Exception {
        row.putDate(column, parseLong(value));
    }

    private long parseLong(DirectByteCharSequence value) throws NumericException {
        return format.parse(value, locale);
    }

    @Override
    public long getTimestamp(DirectByteCharSequence value) throws Exception {
        return parseLong(value) * 1000;
    }

    public DateAdapter of(DateFormat format, DateLocale locale) {
        this.format = format;
        this.locale = locale;
        return this;
    }
}
