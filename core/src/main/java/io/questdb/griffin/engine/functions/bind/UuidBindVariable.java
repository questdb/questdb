/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.std.Mutable;
import io.questdb.std.MutableUuid;

public class UuidBindVariable extends UuidFunction implements ScalarFunction, Mutable {
    final MutableUuid value = new MutableUuid();

    @Override
    public void clear() {
        value.ofNull();
    }

    @Override
    public long getUuidHi(Record rec, long location) {
        return value.getHi();
    }

    @Override
    public long getUuidLo(Record rec, long location) {
        return value.getLo();
    }

    @Override
    public long getUuidLocation(Record rec) {
        return 1;
    }

    void set(long lo, long hi) {
        value.of(lo, hi);
    }
}
