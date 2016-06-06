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
 ******************************************************************************/

package com.questdb.ql.impl.analytic;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;

import java.io.OutputStream;

public interface AnalyticFunction {

    byte get();

    void getBin(OutputStream s);

    DirectInputStream getBin();

    long getBinLen();

    boolean getBool();

    long getDate();

    double getDouble();

    float getFloat();

    CharSequence getFlyweightStr();

    CharSequence getFlyweightStrB();

    int getInt();

    long getLong();

    RecordColumnMetadata getMetadata();

    short getShort();

    void getStr(CharSink sink);

    CharSequence getStr();

    int getStrLen();

    String getSym();

    void reset();

    void scroll(Record record);

    void setStorageFacade(StorageFacade storageFacade);
}
