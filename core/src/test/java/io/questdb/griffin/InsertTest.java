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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.InsertStatement;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.TestBinarySequence;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Rnd;
import io.questdb.std.microtime.DateFormatUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class InsertTest extends AbstractGriffinTest {
    @Test
    public void testSimpleCannedInsert() {
    }

    @Test
    public void testInsertAllByNone() throws Exception {
        testBindVariableInsert(PartitionBy.NONE, () -> 0);
    }

    @Test
    public void testInsertAllByDay() throws Exception {
        testBindVariableInsert(PartitionBy.DAY, new TimestampFunction() {
            private long last = DateFormatUtils.parseDateTime("2019-03-10T00:00:00.0000000");

            @Override
            public long getTimestamp() {
                return last = last + 100000L;
            }
        });
    }

    @Test
    public void testInsertAllByMonth() throws Exception {
        testBindVariableInsert(PartitionBy.MONTH, new TimestampFunction() {
            private long last = DateFormatUtils.parseDateTime("2019-03-10T00:00:00.0000000");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30;
            }
        });
    }

    @Test
    public void testInsertAllByYear() throws Exception {
        testBindVariableInsert(PartitionBy.YEAR, new TimestampFunction() {
            private long last = DateFormatUtils.parseDateTime("2019-03-10T00:00:00.0000000");

            @Override
            public long getTimestamp() {
                return last = last + 100000L * 30 * 12;
            }
        });
    }

    @Test
    public void testInsertNoTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile("create table balances(cust_id int, ccy symbol, balance double)");
            CompiledQuery cq = compiler.compile("insert into balances values (1, 'USD', 356.12)");
            Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
            InsertStatement insert = cq.getInsertStatement();
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), insert.getTableName())) {
                insert.execute(writer, sqlExecutionContext);
                Assert.assertTrue(writer.inTransaction());
                writer.commit();
            }

            String expected = "cust_id\tccy\tbalance\n" +
                    "1\tUSD\t356.120000000000\n";

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), insert.getTableName())) {
                printer.print(reader.getCursor(), reader.getMetadata(), true);
                TestUtils.assertEquals(expected, sink);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();
        });
    }

    private void testBindVariableInsert(
            int partitionBy,
            TimestampFunction timestampFunction
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                CairoTestUtils.createAllTableWithNewTypes(configuration, partitionBy);
                // this is BLOB
                byte[] blob = new byte[500];
                TestBinarySequence bs = new TestBinarySequence();
                bs.of(blob);
                Rnd rnd = new Rnd();

                // this is type declaration to have query compile correctly
                bindVariableService.setInt(0, 0);
                bindVariableService.setShort(1, (short) 10);
                bindVariableService.setByte(2, (byte) 91);
                bindVariableService.setDouble(3, 9.2);
                bindVariableService.setFloat(4, 5.6f);
                bindVariableService.setLong(5, 99901);
                bindVariableService.setStr(6, "hello kitty");
                bindVariableService.setStr(7, "sym?");
                bindVariableService.setBoolean(8, true);
                bindVariableService.setBin(9, bs);
                bindVariableService.setDate(10, 1234L);
                bindVariableService.setLong256(11, 1, 2, 3, 4);
                bindVariableService.setChar(12, 'A');
                bindVariableService.setTimestamp(13, timestampFunction.getTimestamp());

                final CompiledQuery cq = compiler.compile(
                        "insert into all2 (" +
                                "int, " +
                                "short, " +
                                "byte, " +
                                "double, " +
                                "float, " +
                                "long, " +
                                "str, " +
                                "sym, " +
                                "bool, " +
                                "bin, " +
                                "date, " +
                                "long256, " +
                                "chr, " +
                                "timestamp" +
                                ") values (" +
                                "$1, " +
                                "$2, " +
                                "$3, " +
                                "$4, " +
                                "$5, " +
                                "$6, " +
                                "$7, " +
                                "$8, " +
                                "$9, " +
                                "$10, " +
                                "$11, " +
                                "$12, " +
                                "$13, " +
                                "$14)",
                        sqlExecutionContext
                );

                Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                InsertStatement insert = cq.getInsertStatement();
                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), insert.getTableName())) {
                    Assert.assertEquals(writer.getStructureVersion(), insert.getStructureVersion());
                    for (int i = 0; i < 1_000_000; i++) {
                        bindVariableService.setInt(0, rnd.nextInt());
                        bindVariableService.setShort(1, rnd.nextShort());
                        bindVariableService.setByte(2, rnd.nextByte());
                        bindVariableService.setDouble(3, rnd.nextDouble2());
                        bindVariableService.setFloat(4, rnd.nextFloat2());
                        bindVariableService.setLong(5, rnd.nextLong());
                        bindVariableService.setStr(6, rnd.nextChars(6));
                        bindVariableService.setStr(7, rnd.nextChars(1));
                        bindVariableService.setBoolean(8, rnd.nextBoolean());
                        rnd.nextBytes(blob);
                        bindVariableService.setBin(9, bs);
                        bindVariableService.setDate(10, rnd.nextLong());
                        bindVariableService.setLong256(11, rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                        bindVariableService.setChar(12, rnd.nextChar());
                        bindVariableService.setTimestamp(13, timestampFunction.getTimestamp());
                        cq.getInsertStatement().execute(writer, sqlExecutionContext);
                    }
                    writer.commit();
                }

                rnd.reset();
                try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "all2")) {
                    final TableReaderRecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    long time = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextInt(), record.getInt(0));
                        Assert.assertEquals(rnd.nextShort(), record.getShort(1));
                        Assert.assertEquals(rnd.nextByte(), record.getByte(2));
                        Assert.assertEquals(rnd.nextDouble2(), record.getDouble(3), 0.0001);
                        Assert.assertEquals(rnd.nextFloat2(), record.getFloat(4), 0.000001);
                        Assert.assertEquals(rnd.nextLong(), record.getLong(5));
                        TestUtils.assertEquals(rnd.nextChars(6), record.getStr(6));
                        TestUtils.assertEquals(rnd.nextChars(1), record.getSym(7));
                        Assert.assertEquals(rnd.nextBoolean(), record.getBool(8));
                        long ns = System.nanoTime();
                        rnd.nextBytes(blob);
                        BinarySequence binarySequence = record.getBin(9);
                        Assert.assertEquals(blob.length, binarySequence.length());
                        for (int j = 0, m = blob.length; j < m; j++) {
                            Assert.assertEquals(blob[j], binarySequence.byteAt(j));
                        }
                        time += System.nanoTime() - ns;
                        Assert.assertEquals(rnd.nextLong(), record.getDate(10));
                        Long256 long256 = record.getLong256A(11);
                        Assert.assertEquals(rnd.nextLong(), long256.getLong0());
                        Assert.assertEquals(rnd.nextLong(), long256.getLong1());
                        Assert.assertEquals(rnd.nextLong(), long256.getLong2());
                        Assert.assertEquals(rnd.nextLong(), long256.getLong3());
                        Assert.assertEquals(rnd.nextChar(), record.getChar(12));
//                        Assert.assertEquals(0, record.getTimestamp(13));
                    }
                    System.out.println(time);
                }
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @FunctionalInterface
    private interface TimestampFunction {
        long getTimestamp();
    }

}
