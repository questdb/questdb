/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2016 Appsicle
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.questdb.ql;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.io.sink.CharSink;
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
