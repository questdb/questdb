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

package com.questdb.ql;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;

import java.io.OutputStream;

public interface Record {

    byte get(String column);

    byte get(int col);

    void getBin(int col, OutputStream s);

    void getBin(String column, OutputStream s);

    DirectInputStream getBin(String column);

    DirectInputStream getBin(int col);

    long getBinLen(int col);

    boolean getBool(String column);

    boolean getBool(int col);

    long getDate(int col);

    double getDouble(String column);

    double getDouble(int col);

    float getFloat(String column);

    float getFloat(int col);

    CharSequence getFlyweightStr(String column);

    CharSequence getFlyweightStr(int col);

    CharSequence getFlyweightStrB(int col);

    int getInt(String column);

    int getInt(int col);

    long getLong(String column);

    long getLong(int col);

    RecordMetadata getMetadata();

    long getRowId();

    short getShort(int col);

    CharSequence getStr(String column);

    CharSequence getStr(int col);

    void getStr(int col, CharSink sink);

    int getStrLen(int col);

    String getSym(String column);

    String getSym(int col);
}
