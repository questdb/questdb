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

package com.nfsdb.ql.impl.latest;

import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.RowSource;
import com.nfsdb.ql.StorageFacade;

public class MergingRowSource implements RowSource, RowCursor {
    private final RowSource lhs;
    private final RowSource rhs;
    private RowCursor lhc;
    private RowCursor rhc;
    private long nxtl;
    private long nxtr;

    public MergingRowSource(RowSource lhs, RowSource rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.lhs.configure(metadata);
        this.rhs.configure(metadata);
    }

    @Override
    public void prepare(StorageFacade facade) {
        lhs.prepare(facade);
        rhs.prepare(facade);
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        this.lhc = lhs.prepareCursor(slice);
        this.rhc = rhs.prepareCursor(slice);
        nxtl = -1;
        nxtr = -1;
        return this;
    }

    @Override
    public void reset() {
        this.lhs.reset();
        this.rhs.reset();
        this.nxtl = -1;
        this.nxtr = -1;
    }

    @Override
    public boolean hasNext() {
        return nxtl > -1 || lhc.hasNext() || nxtr > -1 || rhc.hasNext();
    }

    @Override
    public long next() {
        long result;

        if (nxtl == -1 && lhc.hasNext()) {
            nxtl = lhc.next();
        }

        if (nxtr == -1 && rhc.hasNext()) {
            nxtr = rhc.next();
        }

        if (nxtr == -1 || (nxtl > -1 && nxtl < nxtr)) {
            result = nxtl;
            nxtl = -1;
        } else {
            result = nxtr;
            nxtr = -1;
        }

        return result;
    }
}
