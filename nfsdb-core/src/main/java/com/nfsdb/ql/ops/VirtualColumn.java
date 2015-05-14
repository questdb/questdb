/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.ql.ops;

import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.RecordSourceState;

import java.io.OutputStream;

public interface VirtualColumn extends RecordColumnMetadata {

    void configureSource(RecordSourceState state);

    byte get();

    void getBin(OutputStream s);

    DirectInputStream getBin();

    boolean getBool();

    long getDate();

    double getDouble();

    float getFloat();

    CharSequence getFlyweightStr();

    int getInt();

    long getLong();

    short getShort();

    CharSequence getStr();

    void getStr(CharSink sink);

    String getSym();

    boolean isConstant();

    void setName(String name);
}
