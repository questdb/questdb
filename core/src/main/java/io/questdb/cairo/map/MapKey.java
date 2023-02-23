/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

    default boolean create() {
        return createValue().isNew();
    }

    MapValue createValue();

    default MapValue createValue2() {
        throw new UnsupportedOperationException();
    }

    default MapValue createValue3() {
        throw new UnsupportedOperationException();
    }

    MapValue findValue();

    default MapValue findValue2() {
        throw new UnsupportedOperationException();
    }

    default MapValue findValue3() {
        throw new UnsupportedOperationException();
    }

    default boolean notFound() {
        return findValue() == null;
    }

    void put(Record record, RecordSink sink);
}
