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

package com.nfsdb.ql.impl.join.hash;

import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.AbstractRecord;
import com.nfsdb.ql.impl.CollectionRecordMetadata;
import com.nfsdb.ql.impl.join.LongMetadata;
import com.nfsdb.std.DirectInputStream;

import java.io.OutputStream;

public class FakeRecord extends AbstractRecord {

    private static final CollectionRecordMetadata metadata = new CollectionRecordMetadata().add(LongMetadata.INSTANCE);
    private long rowId;

    public FakeRecord() {
        super(metadata);
    }

    @Override
    public byte get(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getBin(int col, OutputStream s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DirectInputStream getBin(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBinLen(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBool(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDate(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int col) {
        return rowId;
    }

    @Override
    public long getRowId() {
        return rowId;
    }

    @Override
    public short getShort(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStr(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getStr(int col, CharSink sink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSym(int col) {
        throw new UnsupportedOperationException();
    }

    public FakeRecord of(long rowId) {
        this.rowId = rowId;
        return this;
    }
}
