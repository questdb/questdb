/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.join;

import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Unsafe;

class SplitRecordMetadata extends AbstractRecordMetadata {
    private final int columnCount;
    private final RecordColumnMetadata[] columns;
    private final RecordMetadata a;
    private final RecordMetadata b;
    private final int split;

    SplitRecordMetadata(RecordMetadata a, RecordMetadata b) {
        this.a = a;
        this.b = b;

        int split = a.getColumnCount();
        this.columnCount = split + b.getColumnCount();
        this.columns = new RecordColumnMetadata[columnCount];

        for (int i = 0; i < split; i++) {
            RecordColumnMetadata m = a.getColumnQuick(i);
            columns[i] = m;
        }

        for (int i = 0, c = columnCount - split; i < c; i++) {
            RecordColumnMetadata m = b.getColumnQuick(i);
            columns[i + split] = m;
        }
        this.split = split;
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return columns[index];
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        int index = a.getColumnIndexQuiet(name);
        if (index == -1) {
            index = b.getColumnIndexQuiet(name);
            if (index == -1) {
                return index;
            } else {
                return index + split;
            }
        } else {
            return index;
        }
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return Unsafe.arrayGet(columns, index);
    }

    @Override
    public int getTimestampIndex() {
        return a.getTimestampIndex();
    }
}
