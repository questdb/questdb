/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cutlass.text.types;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.TableWriter;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.str.DirectByteCharSequence;

public final class Long256Adapter extends AbstractTypeAdapter {

    public static final Long256Adapter INSTANCE = new Long256Adapter();

    private Long256Adapter() {
    }

    @Override
    public int getType() {
        return ColumnType.LONG256;
    }

    @Override
    public boolean probe(CharSequence text) {
        final int len = text.length();
        if (len > 2 && ((len & 1) == 0) && len < 67 && text.charAt(0) == '0' && text.charAt(1) == 'x') {
            try {
                Numbers.parseHexLong(text, 2, Math.min(len, 18));
                if (len > 18) {
                    Numbers.parseHexLong(text, 18, Math.min(len, 34));
                }
                if (len > 34) {
                    Numbers.parseHexLong(text, 34, Math.min(len, 42));
                }
                if (len > 42) {
                    Numbers.parseHexLong(text, 42, Math.min(len, 66));
                }
                return true;
            } catch (NumericException ignored) {
                return false;
            }
        }
        return false;
    }

    @Override
    public void write(TableWriter.Row row, int column, DirectByteCharSequence value) {
        row.putLong256(column, value);
    }
}
