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

package io.questdb.griffin.engine.analytic;

import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.impl.analytic.denserank.DenseRankAnalyticFunctionFactory;
import com.questdb.ql.impl.analytic.next.NextAnalyticFunctionFactory;
import com.questdb.ql.impl.analytic.prev.PrevAnalyticFunctionFactory;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.ObjList;

public class AnalyticFunctionFactories {
    private static final CharSequenceObjHashMap<AnalyticFunctionFactory> factories = new CharSequenceObjHashMap<>();

    public static AnalyticFunction newInstance(
            ServerConfiguration configuration,
            String name,
            VirtualColumn valueColumn,
            String valueColumnAlias,
            ObjList<VirtualColumn> partitionBy,
            boolean supportsRowId,
            boolean orderedAnalytic) {

        AnalyticFunctionFactory factory = factories.get(name);
        if (factory != null) {
            return factory.newInstance(configuration, valueColumn, valueColumnAlias, partitionBy, supportsRowId, orderedAnalytic);
        }

        return null;
    }

    static {
        factories.put("next", new NextAnalyticFunctionFactory());
        factories.put("prev", new PrevAnalyticFunctionFactory());
        factories.put("dense_rank", new DenseRankAnalyticFunctionFactory());
    }
}
