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

package io.questdb.griffin.engine.analytic;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.std.Mutable;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

public class AnalyticContextImpl implements AnalyticContext, Mutable {
    private VirtualRecord partitionByRecord;
    private RecordSink partitionBySink;
    private ColumnTypes partitionByKeyTypes;
    private boolean empty = true;
    private boolean ordered;
    private boolean baseSupportsRandomAccess;

    @Override
    public boolean isEmpty() {
        return empty;
    }

    @Override
    public VirtualRecord getPartitionByRecord() {
        return partitionByRecord;
    }

    @Override
    public RecordSink getPartitionBySink() {
        return partitionBySink;
    }

    @Override
    public ColumnTypes getPartitionByKeyTypes() {
        return partitionByKeyTypes;
    }

    @Override
    public boolean isOrdered() {
        return ordered;
    }

    @Override
    public boolean baseSupportsRandomAccess() {
        return baseSupportsRandomAccess;
    }

    @Override
    public void clear() {
        this.empty = true;
        this.partitionByRecord = null;
        this.partitionBySink = null;
        this.partitionByKeyTypes = null;
        this.ordered = false;
        this.baseSupportsRandomAccess = false;
    }

    public void of(
            VirtualRecord partitionByRecord,
            @Nullable RecordSink partitionBySink,
            @Transient @Nullable ColumnTypes partitionByKeyTypes,
            boolean ordered,
            boolean baseSupportsRandomAccess
    ) {
        this.empty = false;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
        this.partitionByKeyTypes = partitionByKeyTypes;
        this.ordered = ordered;
        this.baseSupportsRandomAccess = baseSupportsRandomAccess;
    }
}
