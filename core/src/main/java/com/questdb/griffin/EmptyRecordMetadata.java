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

package com.questdb.griffin;

import com.questdb.common.NoSuchColumnException;
import com.questdb.common.RecordColumnMetadata;
import com.questdb.common.RecordMetadata;

public class EmptyRecordMetadata implements RecordMetadata {

    public static final EmptyRecordMetadata INSTANCE = new EmptyRecordMetadata();

    @Override
    public String getAlias() {
        return null;
    }

    @Override
    public void setAlias(String alias) {

    }

    @Override
    public RecordColumnMetadata getColumn(CharSequence name) {
        return null;
    }

    @Override
    public int getColumnCount() {
        return 0;
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        throw new NoSuchColumnException(name);
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        throw new NoSuchColumnException(name);
    }

    @Override
    public String getColumnName(int index) {
        return null;
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return null;
    }

    @Override
    public int getTimestampIndex() {
        return -1;
    }
}
