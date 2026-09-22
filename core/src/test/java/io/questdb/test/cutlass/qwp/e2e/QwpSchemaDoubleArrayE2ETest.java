/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class QwpSchemaDoubleArrayE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testAllPublicRepresentationsAndEmptyShapes() throws Exception {
        runInContext(port -> {
            execute("create table schema_double_arrays ("
                    + "a1 double[], a2 double[][], a3 double[][][], a4 double[][][][], "
                    + "marker string, ts timestamp) timestamp(ts) partition by day wal");
            try (DoubleArray a4 = new DoubleArray(1, 1, 1, 2);
                 DoubleArray empty4 = new DoubleArray(0, 1, 1, 1);
                 QwpWebSocketSender sender = connectWs(
                         port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                a4.append(9.0).append(10.0);
                sender.table("schema_double_arrays")
                        .doubleArray("a1", new double[]{1.0, -2.5})
                        .doubleArray("a2", new double[][]{{3.0, 4.0}, {5.0, 6.0}})
                        .doubleArray("a3", new double[][][]{{{7.0}, {8.0}}})
                        .doubleArray("a4", a4)
                        .stringColumn("marker", "values")
                        .at(1, ChronoUnit.MICROS);
                sender.table("schema_double_arrays")
                        .doubleArray("a1", new double[0])
                        .doubleArray("a2", new double[0][0])
                        .doubleArray("a3", new double[0][0][0])
                        .doubleArray("a4", empty4)
                        .stringColumn("marker", "empty")
                        .at(2, ChronoUnit.MICROS);
                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(fsn >= 0);
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select marker, a1, a2, a3, a4 from schema_double_arrays order by ts")
                    .noLeakCheck().expectSize().returns("""
                            marker\ta1\ta2\ta3\ta4
                            values\t[1.0,-2.5]\t[[3.0,4.0],[5.0,6.0]]\t[[[7.0],[8.0]]]\t[[[[9.0,10.0]]]]
                            empty\t[]\t[]\t[]\t[]
                            """);
            assertQuery("select dim_length(a1, 1) a1d1, "
                    + "dim_length(a2, 1) a2d1, dim_length(a2, 2) a2d2, "
                    + "dim_length(a3, 1) a3d1, dim_length(a3, 2) a3d2, dim_length(a3, 3) a3d3, "
                    + "dim_length(a4, 1) a4d1, dim_length(a4, 2) a4d2, "
                    + "dim_length(a4, 3) a4d3, dim_length(a4, 4) a4d4 "
                    + "from schema_double_arrays where marker = 'empty'")
                    .noLeakCheck().returns("""
                            a1d1\ta2d1\ta2d2\ta3d1\ta3d2\ta3d3\ta4d1\ta4d2\ta4d3\ta4d4
                            0\t0\t0\t0\t0\t0\t0\t1\t1\t1
                            """);
        });
    }

    @Test
    public void testRankFailureCancelsOnlyPartialRow() throws Exception {
        runInContext(port -> {
            execute("create table schema_double_array_rows (value double[][], marker string, ts timestamp) "
                    + "timestamp(ts) partition by day wal");
            try (QwpWebSocketSender sender = connectWs(
                    port, 0, 0, TimeUnit.MILLISECONDS.toNanos(Integer.MAX_VALUE - 1L))) {
                sender.table("schema_double_array_rows").doubleArray("value", new double[][]{{1.0, 2.0}})
                        .stringColumn("marker", "A").at(1, ChronoUnit.MICROS);
                sender.stringColumn("marker", "failed-B");
                LineSenderSchemaException rankError = Assert.assertThrows(LineSenderSchemaException.class,
                        () -> sender.doubleArray("value", new double[]{99.0}));
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, rankError.getReason());
                Assert.assertTrue(rankError.getMessage(), rankError.getMessage().contains("sourceDims=1"));
                Assert.assertTrue(rankError.getMessage(), rankError.getMessage().contains("targetDims=2"));
                sender.doubleArray("value", new double[][]{{3.0}, {4.0}})
                        .stringColumn("marker", "C").at(2, ChronoUnit.MICROS);

                long fsn = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000));
            }
            drainWalQueue();
            assertQuery("select marker, value from schema_double_array_rows order by ts")
                    .noLeakCheck().expectSize().returns("marker\tvalue\nA\t[[1.0,2.0]]\nC\t[[3.0],[4.0]]\n");
        });
    }
}
