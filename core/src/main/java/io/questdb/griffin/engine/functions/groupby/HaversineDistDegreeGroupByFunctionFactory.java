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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class HaversineDistDegreeGroupByFunctionFactory implements FunctionFactory {

    private static final double EARTH_RADIUS = 6371.088;

    public static double value(double lat1Degrees, double lon1Degrees, double lat2Degrees, double lon2Degrees, double currentTotalDistance) {
        double lat1 = lat1Degrees * Math.PI / 180;
        double lon1 = lon1Degrees * Math.PI / 180;
        double lat2 = lat2Degrees * Math.PI / 180;
        double lon2 = lon2Degrees * Math.PI / 180;
        double halfLatDist = (lat2 - lat1) / 2;
        double halfLonDist = (lon2 - lon1) / 2;
        double a = Math.sin(halfLatDist) * Math.sin(halfLatDist) + Math.cos(lat1) * Math.cos(lat2) * Math.sin(halfLonDist) * Math.sin(halfLonDist);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return currentTotalDistance + EARTH_RADIUS * c;
    }

    @Override
    public String getSignature() {
        return "haversine_dist_deg(DDN)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new HaversineDistDegreeGroupByFunction(args.getQuick(0), args.getQuick(1), args.getQuick(2));
    }
}
