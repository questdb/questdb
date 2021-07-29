/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashExtra;
import org.junit.Test;

public class InsertNullGeoHashTest extends AbstractGriffinTest {

    @Test
    public void testInsertNullGeoHash() throws Exception {
        for (int b = 1; b <= 60; b++) {
            if (b > 1) {
                setUp();
            }
            try {
                int typep = GeoHashExtra.setBitsPrecision(ColumnType.GEOHASH, b);
                final String type = ColumnType.nameOf(typep);
                assertQuery(
                        "geohash\n",
                        "x",
                        String.format("create table x (geohash %s)", type),
                        null,
                        "insert into x select null from long_sequence(1)",
                        "geohash\n" +
                                "\n",
                        true,
                        true,
                        true
                );
            } finally {
                tearDown();
            }
        }
    }
}
