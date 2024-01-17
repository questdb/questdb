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

package io.questdb.griffin;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.TestOnly;

/**
 * Sink used to generate query execution plan.
 * Note: methods like attr(), meta(), child() etc. expect a complete value while
 * calls to val() can be chained to produce a single entry.
 */
public interface PlanSink {

    PlanSink attr(CharSequence name);

    PlanSink child(CharSequence outer, Plannable inner);

    PlanSink child(Plannable p, int order);

    PlanSink child(Plannable p);

    void clear();

    void end();

    SqlExecutionContext getExecutionContext();

    CharSequence getLine(int idx);

    int getLineCount();

    int getOrder();

    @TestOnly
    StringSink getSink();

    boolean getUseBaseMetadata();

    PlanSink meta(CharSequence name);

    void of(RecordCursorFactory factory, SqlExecutionContext executionContext);

    PlanSink optAttr(CharSequence name, Sinkable value);

    PlanSink optAttr(CharSequence name, Plannable value);

    PlanSink optAttr(CharSequence name, Plannable value, boolean useBaseMetadata);

    PlanSink optAttr(CharSequence name, ObjList<? extends Plannable> value, boolean useBaseMetadata);

    PlanSink optAttr(CharSequence name, ObjList<? extends Plannable> value);

    PlanSink putBaseColumnName(int columnIdx);

    PlanSink putBaseColumnNameNoRemap(int columnIdx);

    PlanSink putColumnName(int columnIdx);

    PlanSink type(CharSequence type);

    void useBaseMetadata(boolean b);

    PlanSink val(ObjList<?> list);

    PlanSink val(ObjList<?> list, int from);

    PlanSink val(ObjList<?> list, int from, int to);

    PlanSink val(char c);

    PlanSink val(int i);

    PlanSink val(long l);

    PlanSink val(float f);

    PlanSink val(double d);

    PlanSink val(boolean b);

    PlanSink val(CharSequence cs);

    PlanSink val(Sinkable s);

    PlanSink val(Plannable s);

    PlanSink val(long long0, long long1, long long2, long long3);

    PlanSink val(long hash, int geoHashBits);

    PlanSink valISODate(long l);

    PlanSink valUuid(long lo, long hi);
}
