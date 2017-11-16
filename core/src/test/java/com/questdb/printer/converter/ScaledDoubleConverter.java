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

import com.questdb.printer.JournalPrinter;
import com.questdb.std.Unsafe;

public class ScaledDoubleConverter implements Converter {

    private final int scaleFactor;

    public ScaledDoubleConverter(int scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    public static void appendTo(StringBuilder builder, final double value, int scaleFactor) {
        double d;
        if (value < 0) {
            builder.append('-');
            d = -value;
        } else {
            d = value;
        }

        long factor = (long) Math.pow(10, scaleFactor);
        long scaled = (long) (d * factor + 0.5);

        int scale = scaleFactor + 1;
        while (factor * 10 <= scaled) {
            factor *= 10;
            scale++;
        }
        while (scale > 0) {
            if (scale == scaleFactor)
                builder.append('.');
            long c = scaled / factor % 10;
            factor /= 10;
            builder.append((char) ('0' + c));
            scale--;
        }
    }

    @Override
    public void convert(StringBuilder stringBuilder, JournalPrinter.Field field, Object obj) {
        appendTo(stringBuilder, Unsafe.getUnsafe().getDouble(obj, field.getOffset()), scaleFactor);
    }
}
