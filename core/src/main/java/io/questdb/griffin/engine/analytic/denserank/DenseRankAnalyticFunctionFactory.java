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

package io.questdb.griffin.engine.analytic.denserank;

import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.ql.impl.analytic.AnalyticFunctionFactory;
import com.questdb.ql.ops.VirtualColumn;
import com.questdb.std.ObjList;

public class DenseRankAnalyticFunctionFactory implements AnalyticFunctionFactory {
    @Override
    public AnalyticFunction newInstance(
            ServerConfiguration configuration,
            VirtualColumn valueColumn,
            String valueColumnAlias,
            ObjList<VirtualColumn> partitionBy,
            boolean supportsRowId,
            boolean ordered
    ) {

        if (valueColumn != null) {
            return null;
        }

        if (ordered && partitionBy == null) {
            return new DenseRankOrderedAnalyticFunction(configuration.getDbAnalyticFuncPage(), valueColumnAlias);
        }

        if (ordered) {
            return new DenseRankOPAnalyticFunction(configuration.getDbAnalyticFuncPage(), valueColumnAlias, partitionBy);
        }

        if (partitionBy != null) {
            return new DenseRankPartitionedAnalyticFunction(configuration.getDbAnalyticFuncPage(), valueColumnAlias, partitionBy);
        } else {
            return new DenseRankAnalyticFunction(valueColumnAlias);
        }
    }
}
