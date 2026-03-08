/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.map;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.sql.Record;

public interface MapKey extends RecordSinkSPI {

    // Updates key size for var-sized keys and returns the size.
    long commit();

    void copyFrom(MapKey srcKey);

    default boolean create() {
        return createValue().isNew();
    }

    // Commits implicitly.
    MapValue createValue();

    // Same as createValue(), but doesn't calculate hash code.
    MapValue createValue(long hashCode);

    // Commits implicitly.
    MapValue findValue();

    // Commits implicitly.
    MapValue findValue2();

    // Commits implicitly.
    MapValue findValue3();

    // Must be called after commit.
    long hash();

    default boolean notFound() {
        return findValue() == null;
    }

    void put(Record record, RecordSink sink);

}
