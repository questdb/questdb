/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.lang.cst;

import com.nfsdb.io.sink.CharSink;

import java.io.InputStream;
import java.io.OutputStream;

public interface Record {

    byte get(String column);

    byte get(int col);

    void getBin(int col, OutputStream s);

    void getBin(String column, OutputStream s);

    InputStream getBin(String column);

    InputStream getBin(int col);

    boolean getBool(String column);

    boolean getBool(int col);

    long getDate(int col);

    double getDouble(String column);

    double getDouble(int col);

    float getFloat(String column);

    float getFloat(int col);

    int getInt(String column);

    int getInt(int col);

    long getLong(String column);

    long getLong(int col);

    RecordMetadata getMetadata();

    short getShort(int col);

    CharSequence getStr(String column);

    CharSequence getStr(int col);

    void getStr(int col, CharSink sink);

    String getSym(String column);

    String getSym(int col);
}
