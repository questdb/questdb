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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CastLongToGeoHashFunctionFactoryTest extends AbstractCairoTest {
    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(99889L as geohash(1c))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("cast\n" +
                                    "j\n");

                    assertQuery("select cast(899808896L as geohash(2c))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("cast\n" +
                                    "n0\n");

                    assertQuery("select cast(899808896L as geohash(4c))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("cast\n" +
                                    "3zn0\n");

                    assertQuery("select cast(899808896L as geohash(8c))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("cast\n" +
                                    "00uu3zn0\n");

                    assertQuery("select cast(cast(null as long) as geohash(1c))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("cast\n" +
                                    "\n");

                    assertQuery("select cast(cast(null as long) as geohash(2c))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("cast\n" +
                                    "\n");

                    assertQuery("select cast(cast(null as long) as geohash(4c))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("cast\n" +
                                    "\n");

                    assertQuery("select cast(cast(null as long) as geohash(8c))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("cast\n" +
                                    "\n");
                }
        );
    }
}