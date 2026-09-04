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

package io.questdb.test.cairo;

import io.questdb.cairo.CellRegistry;
import io.questdb.cairo.MapWriter;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.SymbolValueCountCollector;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.CompositeInternerLayout.REGISTRY_NAME;
import static io.questdb.cairo.CompositeInternerLayout.REGISTRY_TXN;

public class CellRegistryTest extends AbstractCairoTest {

    private static final SymbolValueCountCollector NOOP_COLLECTOR = (symbolIndexInTxWriter, count) -> {
    };

    @Test
    public void testGetTupleRoundTripViaReader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createCellFiles(path, 16);

                try (CellRegistry w = openWriterRegistry(path, 0)) {
                    w.internCell(new int[]{5, 9}, 2);
                    w.internCell(new int[]{5, 10}, 2);
                }

                try (
                        SymbolMapReaderImpl readerImpl = new SymbolMapReaderImpl(
                                configuration,
                                path,
                                REGISTRY_NAME,
                                REGISTRY_TXN,
                                2
                        );
                        CellRegistry r = new CellRegistry(readerImpl)
                ) {
                    int[] out = new int[2];
                    r.getTuple(0, out);
                    Assert.assertArrayEquals(new int[]{5, 9}, out);
                    r.getTuple(1, out);
                    Assert.assertArrayEquals(new int[]{5, 10}, out);
                    Assert.assertEquals(2, r.size());
                }
            }
        });
    }

    @Test
    public void testInternIsStableAndDense() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createCellFiles(path, 16);

                try (CellRegistry reg = openWriterRegistry(path, 0)) {
                    Assert.assertEquals(0, reg.internCell(new int[]{5, 9}, 2));
                    Assert.assertEquals(1, reg.internCell(new int[]{5, 10}, 2));
                    Assert.assertEquals(0, reg.internCell(new int[]{5, 9}, 2)); // dedup -> same ordinal
                    Assert.assertEquals(2, reg.size());
                }
            }
        });
    }

    @Test
    public void testWrongSideThrowsIllegalStateException() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                createCellFiles(path, 16);

                try (CellRegistry w = openWriterRegistry(path, 0)) {
                    w.internCell(new int[]{1, 2}, 2);

                    try {
                        w.getTuple(0, new int[2]);
                        Assert.fail("expected IllegalStateException from a write-only CellRegistry");
                    } catch (IllegalStateException expected) {
                        // no reader on this side
                    }
                }

                try (
                        SymbolMapReaderImpl readerImpl = new SymbolMapReaderImpl(
                                configuration,
                                path,
                                REGISTRY_NAME,
                                REGISTRY_TXN,
                                1
                        );
                        CellRegistry r = new CellRegistry(readerImpl)
                ) {
                    try {
                        r.internCell(new int[]{1, 2}, 2);
                        Assert.fail("expected IllegalStateException from a read-only CellRegistry");
                    } catch (IllegalStateException expected) {
                        TestUtils.assertContains(expected.getMessage(), "read-only");
                    }
                }
            }
        });
    }

    private static void createCellFiles(Path path, int symbolCapacity) {
        FilesFacade ff = configuration.getFilesFacade();
        MemoryCMARW mem = Vm.getCMARWInstance();
        MapWriter.createSymbolMapFiles(ff, mem, path, REGISTRY_NAME, REGISTRY_TXN, symbolCapacity, false);
    }

    private static CellRegistry openWriterRegistry(Path path, int symbolCount) {
        SymbolMapWriter writer = new SymbolMapWriter(
                configuration,
                path,
                REGISTRY_NAME,
                REGISTRY_TXN,
                symbolCount,
                -1,
                NOOP_COLLECTOR,
                -1
        );
        return new CellRegistry(writer);
    }
}
