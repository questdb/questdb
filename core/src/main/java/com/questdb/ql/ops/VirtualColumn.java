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

package com.questdb.ql.ops;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;

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

    short getShort(Record rec);

    CharSequence getStr(Record rec);

    void getStr(Record rec, CharSink sink);

    int getStrLen(Record rec);

    String getSym(Record rec);

    boolean isConstant();

    void prepare(StorageFacade facade);

    void setName(String name);
}
