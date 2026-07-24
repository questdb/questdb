/*+*****************************************************************************
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
package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.Misc;

public abstract class AbstractJoinRecordCursorFactory extends AbstractRecordCursorFactory {

    protected final JoinContext joinContext;
    protected RecordCursorFactory masterFactory;
    protected RecordCursorFactory slaveFactory;

    public AbstractJoinRecordCursorFactory(RecordMetadata metadata, JoinContext joinContext, RecordCursorFactory masterFactory, RecordCursorFactory slaveFactory) {
        super(metadata);
        this.joinContext = joinContext;
        this.masterFactory = masterFactory;
        this.slaveFactory = slaveFactory;
    }

    @Override
    public boolean isColumnIntWidthStable(int columnIndex) {
        // A join hands the master factory's live record straight through JoinRecord: for a master
        // column (columnIndex < split) getInt()/getLong() delegate to master.getInt()/getLong(), so a
        // widened INT projection on the master keeps its wide value at long width. The master is never
        // value-materialised in any join (streamed live, or re-read by row id via recordAt), so the
        // master side may delegate to the master factory - just like limit / filter / sort / selection
        // wrappers - and the store then widens the same way it does without the join.
        //
        // The slave side keeps the default true. In a value-materialised join (full hash / outer,
        // keyed AsOf/Lt) the slave lives in a 4-byte chain/map slot where a long-width read would
        // over-read, so true is mandatory; in a light join the slave is a live row-id re-read where
        // delegating would also be correct, but true stays safe (it never over-reads). Widening a
        // live-slave overflowing projection is a pre-existing gap outside this change's master scope.
        if (columnIndex < masterFactory.getMetadata().getColumnCount()) {
            return masterFactory.isColumnIntWidthStable(columnIndex);
        }
        return true;
    }

    @Override
    public boolean isColumnRowStable(int columnIndex) {
        // Paired with isColumnIntWidthStable above, over the same master split. The slave arm
        // answers true because the width sibling answers true there too, so nothing consults this
        // for a slave column: a value-materialised slave lives in a chain/map slot, and reading
        // stored bytes twice gives the same value. The width comment above calls delegating a LIVE
        // slave a deliberate future extension - the day it does, this arm has to delegate with it,
        // because a live slave can be function-backed and then row-unstable.
        if (columnIndex < masterFactory.getMetadata().getColumnCount()) {
            return masterFactory.isColumnRowStable(columnIndex);
        }
        return true;
    }

    protected final Throwable closeJoinOwnersBestEffort() {
        final RecordMetadata metadata = detachMetadata();
        final RecordCursorFactory masterFactory = this.masterFactory;
        this.masterFactory = null;
        final RecordCursorFactory slaveFactory = this.slaveFactory;
        this.slaveFactory = null;
        Throwable failure = Misc.freeIfCloseableBestEffort(null, metadata);
        failure = Misc.freeBestEffort(failure, masterFactory);
        if (slaveFactory != masterFactory) {
            failure = Misc.freeBestEffort(failure, slaveFactory);
        }
        return failure;
    }
}
