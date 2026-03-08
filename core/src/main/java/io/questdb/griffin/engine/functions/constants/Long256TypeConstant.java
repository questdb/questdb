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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.TypeConstant;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.str.CharSink;

public class Long256TypeConstant extends Long256Function implements TypeConstant {
    public static final Long256TypeConstant INSTANCE = new Long256TypeConstant();

    @Override
    public void getLong256(Record rec, CharSink<?> sink) {
        Long256Impl.NULL_LONG256.toSink(sink);
    }

    @Override
    public Long256 getLong256A(Record rec) {
        return Long256Impl.NULL_LONG256;
    }

    @Override
    public Long256 getLong256B(Record rec) {
        return Long256Impl.NULL_LONG256;
    }
}
