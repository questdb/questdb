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

package com.questdb.ql.ops.stat;

import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.map.DirectMapValues;
import com.questdb.ql.ops.Function;
import com.questdb.ql.ops.VirtualColumnFactory;

public class StdDevAggregator extends VarAggregator {
    public static final VirtualColumnFactory<Function> FACTORY = (position, env) -> new StdDevAggregator(position, env.configuration);

    public StdDevAggregator(int position, ServerConfiguration configuration) {
        super(position, configuration);
    }

    @Override
    public void beforeRecord(DirectMapValues values) {
        double r = getResult(values);
        if (r != r) {
            r = Math.sqrt(computeVar(values));
            storeResult(values, r);
        }
    }
}
