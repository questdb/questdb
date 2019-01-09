/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.cutlass.text.typeprobe;

import com.questdb.std.Chars;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateLocale;
import com.questdb.store.ColumnType;

public class BooleanProbe implements TypeProbe {

    @Override
    public DateFormat getDateFormat() {
        return null;
    }

    @Override
    public DateLocale getDateLocale() {
        return null;
    }

    @Override
    public int getType() {
        return ColumnType.BOOLEAN;
    }

    @Override
    public boolean probe(CharSequence text) {
        return Chars.equalsIgnoreCase(text, "true") || Chars.equalsIgnoreCase(text, "false");
    }
}
