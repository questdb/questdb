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

package com.questdb.ql.impl.join.hash;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Numbers;
import com.questdb.ql.AbstractRecord;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;

import java.io.OutputStream;

public class NullRecord extends AbstractRecord {

    public NullRecord(RecordMetadata metadata) {
        super(metadata);
    }

    @Override
    public byte get(int col) {
        return 0;
    }

    @Override
    public void getBin(int col, OutputStream s) {
    }

    @Override
    public DirectInputStream getBin(int col) {
        return null;
    }

    @Override
    public long getBinLen(int col) {
        return -1L;
    }

    @Override
    public boolean getBool(int col) {
        return false;
    }

    @Override
    public long getDate(int col) {
        return Numbers.LONG_NaN;
    }

    @Override
    public double getDouble(int col) {
        return Double.NaN;
    }

    @Override
    public float getFloat(int col) {
        return Float.NaN;
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        return null;
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        return null;
    }

    @Override
    public int getInt(int col) {
        return Numbers.INT_NaN;
    }

    @Override
    public long getLong(int col) {
        return Numbers.LONG_NaN;
    }

    @Override
    public long getRowId() {
        return -1;
    }

    @Override
    public short getShort(int col) {
        return 0;
    }

    @Override
    public CharSequence getStr(int col) {
        return null;
    }

    @Override
    public void getStr(int col, CharSink sink) {

    }

    @Override
    public int getStrLen(int col) {
        return -1;
    }

    @Override
    public String getSym(int col) {
        return null;
    }
}
