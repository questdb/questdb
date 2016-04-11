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

package com.nfsdb.ql.impl.join.asof;

import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.ql.AbstractRecord;
import com.nfsdb.std.DirectInputStream;
import com.nfsdb.store.SymbolTable;

import java.io.OutputStream;

abstract class AbstractMemRecord extends AbstractRecord {

    AbstractMemRecord(RecordMetadata metadata) {
        super(metadata);
    }

    @Override
    public byte get(int col) {
        return Unsafe.getUnsafe().getByte(address(col));
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
        return Unsafe.getBool(address(col));
    }

    @Override
    public long getDate(int col) {
        return Unsafe.getUnsafe().getLong(address(col));
    }

    @Override
    public double getDouble(int col) {
        return Unsafe.getUnsafe().getDouble(address(col));
    }

    @Override
    public float getFloat(int col) {
        return Unsafe.getUnsafe().getFloat(address(col));
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int col) {
        return Unsafe.getUnsafe().getInt(address(col));
    }

    @Override
    public long getLong(int col) {
        return Unsafe.getUnsafe().getLong(address(col));
    }

    @Override
    public long getRowId() {
        return -1;
    }

    @Override
    public short getShort(int col) {
        return Unsafe.getUnsafe().getShort(address(col));
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
        return getSymbolTable(col).value(Unsafe.getUnsafe().getInt(address(col)));
    }

    protected abstract long address(int col);

    protected abstract SymbolTable getSymbolTable(int col);
}
