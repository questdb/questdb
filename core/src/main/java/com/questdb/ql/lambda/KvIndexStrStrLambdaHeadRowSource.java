/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.lambda;

import com.questdb.ql.RecordSource;
import com.questdb.ql.RowSource;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;

public class KvIndexStrStrLambdaHeadRowSource extends KvIndexStrLambdaHeadRowSource {

    public static final LatestByLambdaRowSourceFactory FACTORY = new Factory();

    private KvIndexStrStrLambdaHeadRowSource(String column, RecordSource recordSource, int recordSourceColumn, VirtualColumn filter) {
        super(column, recordSource, recordSourceColumn, filter);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("KvIndexStrStrLambdaHeadRowSource").put(',');
        sink.putQuoted("column").put(':').putQuoted(column).put(',');
        sink.putQuoted("srd").put(':').put(recordSource);
        sink.put('}');
    }

    @Override
    protected CharSequence getKey(Record r, int col) {
        return r.getFlyweightStr(col);
    }

    private static class Factory implements LatestByLambdaRowSourceFactory {
        @Override
        public RowSource newInstance(String column, RecordSource recordSource, int recordSourceColumn, VirtualColumn filter) {
            return new KvIndexStrStrLambdaHeadRowSource(column, recordSource, recordSourceColumn, filter);
        }
    }
}
