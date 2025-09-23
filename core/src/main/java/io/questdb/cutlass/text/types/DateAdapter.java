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
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.Mutable;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.DirectUtf8Sequence;

public class DateAdapter extends AbstractTypeAdapter implements Mutable, TimestampCompatibleAdapter {
    private DateFormat format;
    private DateLocale locale;

    @Override
    public void clear() {
        this.format = null;
        this.locale = null;
    }

    @Override
    public long getTimestamp(DirectUtf8Sequence value, TimestampDriver driver) throws Exception {
        return driver.fromDate(parseLong(value));
    }

    @Override
    public int getType() {
        return ColumnType.DATE;
    }

    public DateAdapter of(DateFormat format, DateLocale locale) {
        this.format = format;
        this.locale = locale;
        return this;
    }

    @Override
    public boolean probe(DirectUtf8Sequence text) {
        try {
            format.parse(text.asAsciiCharSequence(), locale);
            return true;
        } catch (NumericException e) {
            return false;
        }
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectUtf8Sequence value) throws Exception {
        row.putDate(column, parseLong(value));
    }

    private long parseLong(DirectUtf8Sequence value) throws NumericException {
        return format.parse(value.asAsciiCharSequence(), locale);
    }
}
