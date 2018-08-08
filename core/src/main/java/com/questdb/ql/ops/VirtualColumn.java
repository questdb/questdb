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

package com.questdb.ql.ops;

import com.questdb.std.DirectInputStream;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.StorageFacade;

import java.io.OutputStream;

public interface VirtualColumn extends RecordColumnMetadata {

    byte get(Record rec);

    void getBin(Record rec, OutputStream s);

    DirectInputStream getBin(Record rec);

    long getBinLen(Record rec);

    boolean getBool(Record rec);

    long getDate(Record rec);

    double getDouble(Record rec);

    float getFloat(Record rec);

    CharSequence getFlyweightStr(Record rec);

    CharSequence getFlyweightStrB(Record rec);

    int getInt(Record rec);

    long getLong(Record rec);

    int getPosition();

    short getShort(Record rec);

    void getStr(Record rec, CharSink sink);

    int getStrLen(Record rec);

    CharSequence getSym(Record rec);

    boolean isConstant();

    void prepare(StorageFacade facade);

    void setName(String name);
}
