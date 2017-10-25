/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.printer.converter;

import com.questdb.misc.Unsafe;
import com.questdb.printer.JournalPrinter;

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
