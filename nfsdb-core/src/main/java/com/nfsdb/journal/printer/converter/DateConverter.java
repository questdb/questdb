/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.printer.converter;

import com.nfsdb.journal.printer.JournalPrinter;
import com.nfsdb.journal.utils.Unsafe;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class DateConverter extends AbstractConverter {
    private final DateTimeFormatter dateTimeFormatter;

    public DateConverter(JournalPrinter printer) {
        super(printer);
        this.dateTimeFormatter = ISODateTimeFormat.dateTime().withZoneUTC();
    }

    public DateConverter(JournalPrinter printer, DateTimeFormatter dateTimeFormatter) {
        super(printer);
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj) {
        final long millis = Unsafe.getUnsafe().getLong(obj, field.getOffset());
        if (millis == 0) {
            stringBuilder.append(getPrinter().getNullString());
        } else {
            stringBuilder.append(dateTimeFormatter.print(millis));
        }
    }
}
