/*******************************************************************************
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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * {@link PartitionGeometry#addPiece} refuses a piece that does not start strictly above the previous one. Readers
 * look pieces up by {@code tsLo}, so this has to hold in production too, not only under assertions.
 */
public class PartitionGeometryAddPieceTest {

    @Test
    public void testAscendingPiecesAreAccepted() throws Exception {
        final PartitionGeometry geometry = new PartitionGeometry();
        try {
            openUpdate(geometry, 3);
            geometry.addPiece(10, 20, 0, 5, 1, 1);
            // Touching pieces are legal: only the start timestamps must ascend.
            geometry.addPiece(20, 30, 5, 5, 1, 1);
            geometry.addPiece(31, 40, 10, 5, 1, 1);
            Assert.assertEquals(3, pendingRec(geometry));
        } finally {
            geometry.close();
        }
    }

    @Test
    public void testEqualTsLoIsRejected() throws Exception {
        assertRejected(10);
    }

    @Test
    public void testLowerTsLoIsRejected() throws Exception {
        assertRejected(9);
    }

    private static void assertRejected(long secondTsLo) throws Exception {
        final PartitionGeometry geometry = new PartitionGeometry();
        try {
            openUpdate(geometry, 3);
            geometry.addPiece(10, 20, 0, 5, 1, 1);
            try {
                geometry.addPiece(secondTsLo, 30, 5, 5, 1, 1);
                Assert.fail("a piece starting at " + secondTsLo + " after one starting at 10 was accepted");
            } catch (CairoException e) {
                Assert.assertTrue(e.isCritical());
                TestUtils.assertContains(e.getFlyweightMessage(),
                        "pieces must ascend by tsLo [partitionIndex=3, tsLo=" + secondTsLo + ", prevTsLo=10]");
            }
            // The half-built list is dropped, so nothing can commit it.
            Assert.assertEquals(PartitionGeometry.NO_PARTITION, pendingRec(geometry));
        } finally {
            geometry.close();
        }
    }

    // beginUpdate() resolves the partition through a TxReader; the check under test needs only an open update.
    private static void openUpdate(PartitionGeometry geometry, int partitionIndex) throws Exception {
        pendingRecField().setInt(geometry, partitionIndex);
    }

    private static int pendingRec(PartitionGeometry geometry) throws Exception {
        return pendingRecField().getInt(geometry);
    }

    private static Field pendingRecField() throws NoSuchFieldException {
        final Field field = PartitionGeometry.class.getDeclaredField("pendingRec");
        field.setAccessible(true);
        return field;
    }
}
