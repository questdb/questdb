/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.txt.parser.listener.probe;

import com.questdb.misc.Chars;
import com.questdb.txt.ImportedColumnMetadata;
import com.questdb.txt.ImportedColumnType;

public class BooleanProbe implements TypeProbe {

    @Override
    public void getMetadata(ImportedColumnMetadata m) {
        m.importedColumnType = ImportedColumnType.BOOLEAN;
    }

    @Override
    public boolean probe(CharSequence seq) {
        return Chars.equalsIgnoreCase(seq, "true") || Chars.equalsIgnoreCase(seq, "false");
    }
}
