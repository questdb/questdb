/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.printer.converter;

import com.nfsdb.misc.Unsafe;
import com.nfsdb.printer.JournalPrinter;

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
