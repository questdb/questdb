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

package com.questdb.ql.ops;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.ql.impl.NoOpCancellationHandler;
import com.questdb.std.CharSequenceObjHashMap;

public abstract class AbstractRecordSource implements RecordSource {
    private CharSequenceObjHashMap<Parameter> parameterMap;

    @Override
    public Parameter getParam(CharSequence name) {
        Parameter p = parameterMap.get(name);
        if (p == null) {
            throw new JournalRuntimeException("Parameter does not exist");
        }
        return p;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) {
        return prepareCursor(factory, NoOpCancellationHandler.INSTANCE);
    }

    @Override
    public void setParameterMap(CharSequenceObjHashMap<Parameter> map) {
        this.parameterMap = map;
    }
}
