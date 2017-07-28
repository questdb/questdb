/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.ql;

import com.questdb.factory.ReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.impl.NoOpCancellationHandler;
import com.questdb.ql.ops.Parameter;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Sinkable;

import java.io.Closeable;

public interface RecordSource extends Sinkable, Closeable, RecordFactory {

    @Override
    void close();

    RecordMetadata getMetadata();

    Parameter getParam(CharSequence name);

    default RecordCursor prepareCursor(ReaderFactory factory) {
        return prepareCursor(factory, NoOpCancellationHandler.INSTANCE);
    }

    RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler);

    void setParameterMap(CharSequenceObjHashMap<Parameter> map);

    boolean supportsRowIdAccess();
}
