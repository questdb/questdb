/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cairo.map;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.CairoException;
import com.questdb.cairo.ColumnTypes;
import com.questdb.std.Chars;
import com.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class MapFactory {
    public static Map createMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes,
            @Transient @NotNull ColumnTypes valueTypes
    ) {
        CharSequence mapType = configuration.getDefaultMapType();
        if (Chars.equalsLowerCaseAscii(mapType, "fast")) {
            return new FastMap(
                    configuration.getSqlMapPageSize(),
                    keyTypes,
                    valueTypes,
                    configuration.getSqlMapKeyCapacity(),
                    configuration.getSqlFastMapLoadFactor());
        }

        if (Chars.equalsLowerCaseAscii(mapType, "compact")) {
            return new CompactMap(
                    configuration.getSqlMapPageSize(),
                    keyTypes,
                    valueTypes,
                    configuration.getSqlMapKeyCapacity(),
                    configuration.getSqlCompactMapLoadFactor()
            );
        }
        throw CairoException.instance(0).put("unknown map type: ").put(mapType);
    }

    public static Map createMap(
            CairoConfiguration configuration,
            @Transient @NotNull ColumnTypes keyTypes) {
        CharSequence mapType = configuration.getDefaultMapType();
        if (Chars.equalsLowerCaseAscii(mapType, "fast")) {
            return new FastMap(
                    configuration.getSqlMapPageSize(),
                    keyTypes,
                    configuration.getSqlMapKeyCapacity(),
                    configuration.getSqlFastMapLoadFactor());
        }

        if (Chars.equalsLowerCaseAscii(mapType, "compact")) {
            return new CompactMap(
                    configuration.getSqlMapPageSize(),
                    keyTypes,
                    null, // todo: test
                    configuration.getSqlMapKeyCapacity(),
                    configuration.getSqlCompactMapLoadFactor()
            );
        }
        throw CairoException.instance(0).put("unknown map type: ").put(mapType);
    }

}
