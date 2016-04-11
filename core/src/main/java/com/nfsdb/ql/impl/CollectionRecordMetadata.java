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

package com.nfsdb.ql.impl;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.misc.Chars;
import com.nfsdb.std.ObjHashSet;
import com.nfsdb.std.ObjList;

public class CollectionRecordMetadata extends AbstractRecordMetadata {
    private final ObjList<RecordColumnMetadata> columns = new ObjList<>();
    private final ObjHashSet<String> columnNames = new ObjHashSet<>();

    public CollectionRecordMetadata add(RecordColumnMetadata meta) {
        if (columnNames.add(meta.getName())) {
            columns.add(meta);
            return this;
        } else {
            throw new JournalRuntimeException("Duplicate column name");
        }
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return columns.get(index);
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (Chars.equals(columns.getQuick(i).getName(), name)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return columns.getQuick(index);
    }

    @Override
    public int getTimestampIndex() {
        return -1;
    }

    public ObjHashSet<String> getColumnNames() {
        return columnNames;
    }
}
