/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.ops;

import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.std.DirectInputStream;

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
