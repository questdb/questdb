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

package com.questdb.cutlass.text;

import com.questdb.cairo.ColumnType;
import com.questdb.std.Mutable;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateLocale;

public class TextMetadata implements Mutable {

    public int type;
    public DateFormat dateFormat;
    public DateLocale dateLocale;
    public CharSequence name = "";

    @Override
    public void clear() {
        name = "";
    }

    public void copyTo(TextMetadata _m) {
        _m.type = this.type;
        if (this.type == ColumnType.DATE) {

            if (this.dateFormat != null) {
                _m.dateFormat = this.dateFormat;
            }

            if (this.dateLocale != null) {
                _m.dateLocale = this.dateLocale;
            }

        } else {
            _m.dateFormat = this.dateFormat;
            _m.dateLocale = this.dateLocale;
        }
    }

    @Override
    public String toString() {
        return "TextMetadata{" +
                "type=" + ColumnType.nameOf(type) +
                ", dateLocale=" + (dateLocale == null ? null : dateLocale.getId()) +
                ", name=" + name +
                '}';
    }
}

