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

package com.nfsdb.ql.ops.col;

import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.AbstractVirtualColumn;
import com.nfsdb.std.DirectInputStream;
import com.nfsdb.store.ColumnType;

import java.io.OutputStream;

public class BinaryRecordSourceColumn extends AbstractVirtualColumn {
    private final int index;

    public BinaryRecordSourceColumn(int index) {
        super(ColumnType.DOUBLE);
        this.index = index;
    }

    @Override
    public void getBin(Record rec, OutputStream s) {
        rec.getBin(index, s);
    }

    @Override
    public DirectInputStream getBin(Record rec) {
        return rec.getBin(index);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public void prepare(StorageFacade facade) {
    }
}
