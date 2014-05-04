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

import java.util.regex.Pattern;

public class StripCRLFStringConverter extends AbstractConverter {

    private static final Pattern CR = Pattern.compile("\n", Pattern.LITERAL);
    private static final Pattern LF = Pattern.compile("\r", Pattern.LITERAL);

    public StripCRLFStringConverter(JournalPrinter printer) {
        super(printer);
    }

    @Override
    public void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj) {
        String s;

        if (field.getOffset() == -1) {
            s = obj.toString();
        } else {
            s = (String) Unsafe.getUnsafe().getObject(obj, field.getOffset());
        }

        if (s == null) {
            stringBuilder.append(getPrinter().getNullString());
        } else {
            stringBuilder.append(LF.matcher(CR.matcher(s).replaceAll(" ")).replaceAll(""));
        }
    }
}
