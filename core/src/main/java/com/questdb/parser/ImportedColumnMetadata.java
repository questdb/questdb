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

package com.questdb.parser;

import com.questdb.std.Mutable;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateLocale;
import com.questdb.store.ColumnType;

public class ImportedColumnMetadata implements Mutable {

    public int importedColumnType;
    public CharSequence pattern;
    public DateFormat dateFormat;
    public DateLocale dateLocale;
    public CharSequence name;

    @Override
    public void clear() {
    }

    public void copyTo(ImportedColumnMetadata _m) {
        _m.importedColumnType = this.importedColumnType;
        if (this.importedColumnType == ColumnType.DATE) {
            if (this.pattern != null) {
                _m.pattern = this.pattern;
            }

            if (this.dateFormat != null) {
                _m.dateFormat = this.dateFormat;
            }

            if (this.dateLocale != null) {
                _m.dateLocale = this.dateLocale;
            }

        } else {
            _m.pattern = this.pattern;
            _m.dateFormat = this.dateFormat;
            _m.dateLocale = this.dateLocale;
        }
    }

    @Override
    public String toString() {
        return "ImportedColumnMetadata{" +
                "importedColumnType=" + ColumnType.nameOf(importedColumnType) +
                ", pattern=" + pattern +
                ", dateLocale=" + (dateLocale == null ? null : dateLocale.getId()) +
                ", name=" + name +
                '}';
    }
}

