/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.ql;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.std.DirectInputStream;

import java.io.OutputStream;

public abstract class AbstractRecord implements Record {

    protected final RecordMetadata metadata;

    protected AbstractRecord(RecordMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public byte get(String column) {
        return get(metadata.getColumnIndex(column));
    }

    @Override
    public void getBin(String column, OutputStream s) {
        getBin(metadata.getColumnIndex(column), s);
    }

    @Override
    public DirectInputStream getBin(String column) {
        return getBin(metadata.getColumnIndex(column));
    }

    @Override
    public boolean getBool(String column) {
        return getBool(metadata.getColumnIndex(column));
    }

    @Override
    public double getDouble(String column) {
        return getDouble(metadata.getColumnIndex(column));
    }

    @Override
    public float getFloat(String column) {
        return getFloat(metadata.getColumnIndex(column));
    }

    @Override
    public CharSequence getFlyweightStr(String column) {
        return getFlyweightStr(metadata.getColumnIndex(column));
    }

    @Override
    public int getInt(String column) {
        return getInt(metadata.getColumnIndex(column));
    }

    @Override
    public long getLong(String column) {
        return getLong(metadata.getColumnIndex(column));
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public CharSequence getStr(String column) {
        return getStr(metadata.getColumnIndex(column));
    }

    @Override
    public String getSym(String column) {
        return getSym(metadata.getColumnIndex(column));
    }
}
