/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryCompiler;
import com.questdb.parser.sql.QueryError;
import com.questdb.std.ByteBuffers;
import com.questdb.std.DirectInputStream;
import com.questdb.std.Rnd;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.Dates;
import com.questdb.store.*;
import com.questdb.store.factory.configuration.JournalMetadata;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

public class DDLTests extends AbstractTest {

    private static final QueryCompiler compiler = new QueryCompiler();

    @Test
    public void testBadIntBuckets() throws Exception {
        try {
            exec("create table x (a INT index buckets -1, b BYTE, t TIMESTAMP, x SYMBOL) partition by MONTH");
            Assert.fail();
        } catch (ParserException ignore) {
            // good, pass
        }
    }

    public void testCast(int from, int to) throws Exception {
        int n = 100;
        try (JournalWriter w1 = compiler.createWriter(getFactory(), "create table y (a " + ColumnType.nameOf(from) + ") record hint 100")) {
            Rnd rnd = new Rnd();
            for (int i = 0; i < n; i++) {
                JournalEntryWriter ew = w1.entryWriter();
                switch (from) {
                    case ColumnType.INT:
                        ew.putInt(0, rnd.nextInt());
                        break;
                    case ColumnType.LONG:
                        ew.putLong(0, rnd.nextLong());
                        break;
                    case ColumnType.DATE:
                        ew.putDate(0, rnd.nextLong());
                        break;
                    case ColumnType.BYTE:
                        ew.put(0, rnd.nextByte());
                        break;
                    case ColumnType.SHORT:
                        ew.putShort(0, rnd.nextShort());
                        break;
                    case ColumnType.FLOAT:
                        ew.putFloat(0, rnd.nextFloat());
                        break;
                    case ColumnType.DOUBLE:
                        ew.putDouble(0, rnd.nextDouble());
                        break;
                    case ColumnType.SYMBOL:
                        ew.putSym(0, rnd.nextChars(10));
                        break;
                    case ColumnType.STRING:
                        ew.putStr(0, rnd.nextChars(10));
                        break;
                    default:
                        break;
                }
                ew.append();
            }
            w1.commit();
        }

        exec("create table x as (y), cast(a as " + ColumnType.nameOf(to) + ") record hint 100");

        try (RecordSource rs = compiler.compile(getFactory(), "x")) {
            Rnd rnd = new Rnd();
            Assert.assertEquals(to, rs.getMetadata().getColumnQuick(0).getType());
            RecordCursor cursor = rs.prepareCursor(getFactory());
            try {
                while (cursor.hasNext()) {
                    switch (from) {
                        case ColumnType.INT:
                            switch (to) {
                                case ColumnType.SHORT:
                                    Assert.assertEquals((short) rnd.nextInt(), cursor.next().getShort(0));
                                    break;
                                case ColumnType.LONG:
                                    Assert.assertEquals((long) rnd.nextInt(), cursor.next().getLong(0));
                                    break;
                                case ColumnType.BYTE:
                                    Assert.assertEquals((byte) rnd.nextInt(), cursor.next().getByte(0));
                                    break;
                                case ColumnType.FLOAT:
                                    Assert.assertEquals((float) rnd.nextInt(), cursor.next().getFloat(0), 0.000000001f);
                                    break;
                                case ColumnType.DOUBLE:
                                    Assert.assertEquals((double) rnd.nextInt(), cursor.next().getDouble(0), 0.000000001);
                                    break;
                                case ColumnType.DATE:
                                    Assert.assertEquals((long) rnd.nextInt(), cursor.next().getDate(0));
                                    break;
                                case ColumnType.INT:
                                    Assert.assertEquals(rnd.nextInt(), cursor.next().getInt(0));
                                    break;
                                default:
                                    break;
                            }
                            break;
                        case ColumnType.LONG:
                        case ColumnType.DATE:
                            switch (to) {
                                case ColumnType.SHORT:
                                    Assert.assertEquals((short) rnd.nextLong(), cursor.next().getShort(0));
                                    break;
                                case ColumnType.LONG:
                                    Assert.assertEquals(rnd.nextLong(), cursor.next().getLong(0));
                                    break;
                                case ColumnType.BYTE:
                                    Assert.assertEquals((byte) rnd.nextLong(), cursor.next().getByte(0));
                                    break;
                                case ColumnType.FLOAT:
                                    Assert.assertEquals((float) rnd.nextLong(), cursor.next().getFloat(0), 0.000000001f);
                                    break;
                                case ColumnType.DOUBLE:
                                    Assert.assertEquals((double) rnd.nextLong(), cursor.next().getDouble(0), 0.000000001);
                                    break;
                                case ColumnType.DATE:
                                    Assert.assertEquals(rnd.nextLong(), cursor.next().getDate(0));
                                    break;
                                case ColumnType.INT:
                                    Assert.assertEquals((int) rnd.nextLong(), cursor.next().getInt(0));
                                    break;
                                default:
                                    break;
                            }
                            break;
                        case ColumnType.BYTE:
                            switch (to) {
                                case ColumnType.SHORT:
                                    Assert.assertEquals((short) rnd.nextByte(), cursor.next().getShort(0));
                                    break;
                                case ColumnType.LONG:
                                    Assert.assertEquals((long) rnd.nextByte(), cursor.next().getLong(0));
                                    break;
                                case ColumnType.BYTE:
                                    Assert.assertEquals(rnd.nextByte(), cursor.next().getByte(0));
                                    break;
                                case ColumnType.FLOAT:
                                    Assert.assertEquals((float) rnd.nextByte(), cursor.next().getFloat(0), 0.000000001f);
                                    break;
                                case ColumnType.DOUBLE:
                                    Assert.assertEquals((double) rnd.nextByte(), cursor.next().getDouble(0), 0.000000001);
                                    break;
                                case ColumnType.DATE:
                                    Assert.assertEquals((long) rnd.nextByte(), cursor.next().getDate(0));
                                    break;
                                case ColumnType.INT:
                                    Assert.assertEquals((int) rnd.nextByte(), cursor.next().getInt(0));
                                    break;
                                default:
                                    break;
                            }
                            break;
                        case ColumnType.SHORT:
                            switch (to) {
                                case ColumnType.SHORT:
                                    Assert.assertEquals(rnd.nextShort(), cursor.next().getShort(0));
                                    break;
                                case ColumnType.LONG:
                                    Assert.assertEquals((long) rnd.nextShort(), cursor.next().getLong(0));
                                    break;
                                case ColumnType.BYTE:
                                    Assert.assertEquals((byte) rnd.nextShort(), cursor.next().getByte(0));
                                    break;
                                case ColumnType.FLOAT:
                                    Assert.assertEquals((float) rnd.nextShort(), cursor.next().getFloat(0), 0.000000001f);
                                    break;
                                case ColumnType.DOUBLE:
                                    Assert.assertEquals((double) rnd.nextShort(), cursor.next().getDouble(0), 0.000000001);
                                    break;
                                case ColumnType.DATE:
                                    Assert.assertEquals((long) rnd.nextShort(), cursor.next().getDate(0));
                                    break;
                                case ColumnType.INT:
                                    Assert.assertEquals((int) rnd.nextShort(), cursor.next().getInt(0));
                                    break;
                                default:
                                    break;
                            }
                            break;
                        case ColumnType.FLOAT:
                            switch (to) {
                                case ColumnType.SHORT:
                                    Assert.assertEquals((short) rnd.nextFloat(), cursor.next().getShort(0));
                                    break;
                                case ColumnType.LONG:
                                    Assert.assertEquals((long) rnd.nextFloat(), cursor.next().getLong(0));
                                    break;
                                case ColumnType.BYTE:
                                    Assert.assertEquals((byte) rnd.nextFloat(), cursor.next().getByte(0));
                                    break;
                                case ColumnType.FLOAT:
                                    Assert.assertEquals(rnd.nextFloat(), cursor.next().getFloat(0), 0.000000001f);
                                    break;
                                case ColumnType.DOUBLE:
                                    Assert.assertEquals((double) rnd.nextFloat(), cursor.next().getDouble(0), 0.000000001);
                                    break;
                                case ColumnType.DATE:
                                    Assert.assertEquals((long) rnd.nextFloat(), cursor.next().getDate(0));
                                    break;
                                case ColumnType.INT:
                                    Assert.assertEquals((int) rnd.nextFloat(), cursor.next().getInt(0));
                                    break;
                                default:
                                    break;
                            }
                            break;
                        case ColumnType.DOUBLE:
                            switch (to) {
                                case ColumnType.SHORT:
                                    Assert.assertEquals((short) rnd.nextDouble(), cursor.next().getShort(0));
                                    break;
                                case ColumnType.LONG:
                                    Assert.assertEquals((long) rnd.nextDouble(), cursor.next().getLong(0));
                                    break;
                                case ColumnType.BYTE:
                                    Assert.assertEquals((byte) rnd.nextDouble(), cursor.next().getByte(0));
                                    break;
                                case ColumnType.FLOAT:
                                    Assert.assertEquals((float) rnd.nextDouble(), cursor.next().getFloat(0), 0.000000001f);
                                    break;
                                case ColumnType.DOUBLE:
                                    Assert.assertEquals(rnd.nextDouble(), cursor.next().getDouble(0), 0.000000001);
                                    break;
                                case ColumnType.DATE:
                                    Assert.assertEquals((long) rnd.nextDouble(), cursor.next().getDate(0));
                                    break;
                                case ColumnType.INT:
                                    Assert.assertEquals((int) rnd.nextDouble(), cursor.next().getInt(0));
                                    break;
                                default:
                                    break;
                            }
                            break;
                        case ColumnType.STRING:
                            switch (to) {
                                case ColumnType.SYMBOL:
                                    TestUtils.assertEquals(rnd.nextChars(10), cursor.next().getSym(0));
                                    break;
                                default:
                                    TestUtils.assertEquals(rnd.nextChars(10), cursor.next().getFlyweightStr(0));
                                    break;
                            }
                            break;
                        case ColumnType.SYMBOL:
                            switch (to) {
                                case ColumnType.STRING:
                                    TestUtils.assertEquals(rnd.nextChars(10), cursor.next().getFlyweightStr(0));
                                    break;
                                default:
                                    TestUtils.assertEquals(rnd.nextChars(10), cursor.next().getSym(0));
                                    break;
                            }
                            break;
                        default:
                            break;
                    }
                }
            } finally {
                cursor.releaseCursor();
            }
        }
    }

    @Test
    public void testCastByteAsByte() throws Exception {
        testCast(ColumnType.BYTE, ColumnType.BYTE);
    }

    @Test
    public void testCastByteAsDate() throws Exception {
        testCast(ColumnType.BYTE, ColumnType.DATE);
    }

    @Test
    public void testCastByteAsDouble() throws Exception {
        testCast(ColumnType.BYTE, ColumnType.DOUBLE);
    }

    @Test
    public void testCastByteAsFloat() throws Exception {
        testCast(ColumnType.BYTE, ColumnType.FLOAT);
    }

    @Test
    public void testCastByteAsInt() throws Exception {
        testCast(ColumnType.BYTE, ColumnType.INT);
    }

    @Test
    public void testCastByteAsLong() throws Exception {
        testCast(ColumnType.BYTE, ColumnType.LONG);
    }

    @Test
    public void testCastByteAsShort() throws Exception {
        testCast(ColumnType.BYTE, ColumnType.SHORT);
    }

    @Test
    public void testCastDateAsByte() throws Exception {
        testCast(ColumnType.DATE, ColumnType.BYTE);
    }

    @Test
    public void testCastDateAsDate() throws Exception {
        testCast(ColumnType.DATE, ColumnType.DATE);
    }

    @Test
    public void testCastDateAsDouble() throws Exception {
        testCast(ColumnType.DATE, ColumnType.DOUBLE);
    }

    @Test
    public void testCastDateAsFloat() throws Exception {
        testCast(ColumnType.DATE, ColumnType.FLOAT);
    }

    @Test
    public void testCastDateAsInt() throws Exception {
        testCast(ColumnType.DATE, ColumnType.INT);
    }

    @Test
    public void testCastDateAsLong() throws Exception {
        testCast(ColumnType.DATE, ColumnType.LONG);
    }

    @Test
    public void testCastDateAsShort() throws Exception {
        testCast(ColumnType.DATE, ColumnType.SHORT);
    }

    @Test
    public void testCastDoubleAsByte() throws Exception {
        testCast(ColumnType.DOUBLE, ColumnType.BYTE);
    }

    @Test
    public void testCastDoubleAsDate() throws Exception {
        testCast(ColumnType.DOUBLE, ColumnType.DATE);
    }

    @Test
    public void testCastDoubleAsDouble() throws Exception {
        testCast(ColumnType.DOUBLE, ColumnType.DOUBLE);
    }

    @Test
    public void testCastDoubleAsFloat() throws Exception {
        testCast(ColumnType.DOUBLE, ColumnType.FLOAT);
    }

    @Test
    public void testCastDoubleAsInt() throws Exception {
        testCast(ColumnType.DOUBLE, ColumnType.INT);
    }

    @Test
    public void testCastDoubleAsLong() throws Exception {
        testCast(ColumnType.DOUBLE, ColumnType.LONG);
    }

    @Test
    public void testCastDoubleAsShort() throws Exception {
        testCast(ColumnType.DOUBLE, ColumnType.SHORT);
    }

    @Test
    public void testCastFloatAsByte() throws Exception {
        testCast(ColumnType.FLOAT, ColumnType.BYTE);
    }

    @Test
    public void testCastFloatAsDate() throws Exception {
        testCast(ColumnType.FLOAT, ColumnType.DATE);
    }

    @Test
    public void testCastFloatAsDouble() throws Exception {
        testCast(ColumnType.FLOAT, ColumnType.DOUBLE);
    }

    @Test
    public void testCastFloatAsFloat() throws Exception {
        testCast(ColumnType.FLOAT, ColumnType.FLOAT);
    }

    @Test
    public void testCastFloatAsInt() throws Exception {
        testCast(ColumnType.FLOAT, ColumnType.INT);
    }

    @Test
    public void testCastFloatAsLong() throws Exception {
        testCast(ColumnType.FLOAT, ColumnType.LONG);
    }

    @Test
    public void testCastFloatAsShort() throws Exception {
        testCast(ColumnType.FLOAT, ColumnType.SHORT);
    }

    @Test
    public void testCastIntAsByte() throws Exception {
        testCast(ColumnType.INT, ColumnType.BYTE);
    }

    @Test
    public void testCastIntAsDate() throws Exception {
        testCast(ColumnType.INT, ColumnType.DATE);
    }

    @Test
    public void testCastIntAsDouble() throws Exception {
        testCast(ColumnType.INT, ColumnType.DOUBLE);
    }

    @Test
    public void testCastIntAsFloat() throws Exception {
        testCast(ColumnType.INT, ColumnType.FLOAT);
    }

    @Test
    public void testCastIntAsInt() throws Exception {
        testCast(ColumnType.INT, ColumnType.INT);
    }

    @Test
    public void testCastIntAsLong() throws Exception {
        testCast(ColumnType.INT, ColumnType.LONG);
    }

    @Test
    public void testCastIntAsShort() throws Exception {
        testCast(ColumnType.INT, ColumnType.SHORT);
    }

    @Test
    public void testCastLongAsByte() throws Exception {
        testCast(ColumnType.LONG, ColumnType.BYTE);
    }

    @Test
    public void testCastLongAsDate() throws Exception {
        testCast(ColumnType.LONG, ColumnType.DATE);
    }

    @Test
    public void testCastLongAsDouble() throws Exception {
        testCast(ColumnType.LONG, ColumnType.DOUBLE);
    }

    @Test
    public void testCastLongAsFloat() throws Exception {
        testCast(ColumnType.LONG, ColumnType.FLOAT);
    }

    @Test
    public void testCastLongAsInt() throws Exception {
        testCast(ColumnType.LONG, ColumnType.INT);
    }

    @Test
    public void testCastLongAsLong() throws Exception {
        testCast(ColumnType.LONG, ColumnType.LONG);
    }

    @Test
    public void testCastLongAsShort() throws Exception {
        testCast(ColumnType.LONG, ColumnType.SHORT);
    }

    @Test
    public void testCastShortAsByte() throws Exception {
        testCast(ColumnType.SHORT, ColumnType.BYTE);
    }

    @Test
    public void testCastShortAsDate() throws Exception {
        testCast(ColumnType.SHORT, ColumnType.DATE);
    }

    @Test
    public void testCastShortAsDouble() throws Exception {
        testCast(ColumnType.SHORT, ColumnType.DOUBLE);
    }

    @Test
    public void testCastShortAsFloat() throws Exception {
        testCast(ColumnType.SHORT, ColumnType.FLOAT);
    }

    @Test
    public void testCastShortAsInt() throws Exception {
        testCast(ColumnType.SHORT, ColumnType.INT);
    }

    @Test
    public void testCastShortAsLong() throws Exception {
        testCast(ColumnType.SHORT, ColumnType.LONG);
    }

    @Test
    public void testCastShortAsShort() throws Exception {
        testCast(ColumnType.SHORT, ColumnType.SHORT);
    }

    @Test
    public void testCastStrAsStr() throws Exception {
        testCast(ColumnType.STRING, ColumnType.STRING);
    }

    @Test
    public void testCastStrAsSym() throws Exception {
        testCast(ColumnType.STRING, ColumnType.SYMBOL);
    }

    @Test
    public void testCastSymAsStr() throws Exception {
        testCast(ColumnType.SYMBOL, ColumnType.STRING);
    }

    @Test
    public void testCastSymAsSym() throws Exception {
        testCast(ColumnType.SYMBOL, ColumnType.SYMBOL);
    }

    @Test
    public void testCreateAllFieldTypes() throws Exception {
        exec("create table x (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING, y BOOLEAN) timestamp(t) partition by MONTH record hint 100");

        // validate journal
        try (Journal r = getFactory().reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(12, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());

            Assert.assertEquals(ColumnType.SHORT, m.getColumn("c").getType());
            Assert.assertEquals(ColumnType.LONG, m.getColumn("d").getType());
            Assert.assertEquals(ColumnType.FLOAT, m.getColumn("e").getType());
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumn("f").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("g").getType());
            Assert.assertEquals(ColumnType.BINARY, m.getColumn("h").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertEquals(ColumnType.BOOLEAN, m.getColumn("y").getType());

            Assert.assertEquals(8, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateAsSelect() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try (JournalWriter w = compiler.createWriter(getFactory(), "create table x as (y order by t)")) {
            JournalMetadata m = w.getMetadata();
            Assert.assertEquals(11, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());

            Assert.assertEquals(ColumnType.SHORT, m.getColumn("c").getType());
            Assert.assertEquals(ColumnType.LONG, m.getColumn("d").getType());
            Assert.assertEquals(ColumnType.FLOAT, m.getColumn("e").getType());
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumn("f").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("g").getType());
            Assert.assertEquals(ColumnType.BINARY, m.getColumn("h").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertEquals(8, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.NONE, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateAsSelectAll() throws Exception {
        int N = 50;
        try (JournalWriter w = compiler.createWriter(getFactory(), "create table x (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING, y BOOLEAN) timestamp(t) record hint 100")) {
            Rnd rnd = new Rnd();

            long t = DateFormatUtils.parseDateTime("2016-01-10T00:00:00.000Z");

            for (int i = 0; i < N; i++) {
                JournalEntryWriter ew = w.entryWriter(t += Dates.DAY_MILLIS);
                ew.putInt(0, i);
                ew.put(1, (byte) rnd.nextInt());
                ew.putShort(2, (short) rnd.nextInt());
                ew.putLong(3, rnd.nextLong());
                ew.putFloat(4, rnd.nextFloat());
                ew.putDouble(5, rnd.nextDouble());
                ew.putDate(6, rnd.nextLong());
                ew.putNull(7);
                ew.putSym(9, rnd.nextChars(1));
                ew.putStr(10, rnd.nextChars(10));
                ew.putBool(11, rnd.nextBoolean());
                ew.append();
            }
            w.commit();
        }

        exec("create table y as (x) partition by MONTH");

        try (Journal r = getFactory().reader("y")) {
            Assert.assertEquals(2, r.getPartitionCount());
        }

        int count = 0;
        try (RecordSource rs = compiler.compile(getFactory(), "y")) {
            RecordCursor cursor = rs.prepareCursor(getFactory());

            try {
                Rnd rnd = new Rnd();
                while (cursor.hasNext()) {
                    Record rec = cursor.next();
                    Assert.assertEquals(count, rec.getInt(0));
                    Assert.assertTrue((byte) rnd.nextInt() == rec.getByte(1));
                    Assert.assertEquals((short) rnd.nextInt(), rec.getShort(2));
                    Assert.assertEquals(rnd.nextLong(), rec.getLong(3));
                    Assert.assertEquals(rnd.nextFloat(), rec.getFloat(4), 0.00001f);
                    Assert.assertEquals(rnd.nextDouble(), rec.getDouble(5), 0.00000000001);
                    Assert.assertEquals(rnd.nextLong(), rec.getDate(6));
                    Assert.assertNull(rec.getBin(7));
                    TestUtils.assertEquals(rnd.nextChars(1), rec.getSym(9));
                    TestUtils.assertEquals(rnd.nextChars(10), rec.getFlyweightStr(10));
                    Assert.assertEquals(rnd.nextBoolean(), rec.getBool(11));
                    count++;
                }
            } finally {
                cursor.releaseCursor();
            }
        }
    }

    @Test
    public void testCreateAsSelectBadHint() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) record hint 100");
        try {
            exec("create table x as (y order by t) record hint 1000000000000000000000000000");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(45, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectBadIndex() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) record hint 100");
        try {
            exec("create table x as (y order by t), index(e) record hint 100");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(40, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectBadIndex2() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) record hint 100");
        try {
            exec("create table x as (y order by t), index(e2) record hint 100");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(40, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectBadTimestamp() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) record hint 100");
        try {
            exec("create table x as (y order by t) timestamp(c) partition by MONTH record hint 100");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(43, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectBadTimestamp2() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) record hint 100");
        try {
            exec("create table x as (y order by t) timestamp(c2) partition by MONTH record hint 100");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(43, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectBin() throws Exception {
        int N = 10000;
        int SZ = 4096;
        ByteBuffer buf = ByteBuffer.allocateDirect(SZ);
        try {
            long addr = ByteBuffers.getAddress(buf);
            try (JournalWriter w = compiler.createWriter(getFactory(), "create table x (a INT, b BINARY)")) {
                Rnd rnd = new Rnd();

                for (int i = 0; i < N; i++) {
                    long p = addr;
                    int n = (rnd.nextPositiveInt() % (SZ - 1)) / 8;
                    for (int j = 0; j < n; j++) {
                        Unsafe.getUnsafe().putLong(p, rnd.nextLong());
                        p += 8;
                    }

                    buf.limit(n * 8);
                    JournalEntryWriter ew = w.entryWriter();
                    ew.putInt(0, i);
                    ew.putBin(1, buf);
                    ew.append();
                    buf.clear();
                }
                w.commit();
            }

            exec("create table y as (x)");

            int count = 0;
            try (RecordSource rs = compiler.compile(getFactory(), "y")) {
                RecordCursor cursor = rs.prepareCursor(getFactory());

                try {
                    Rnd rnd = new Rnd();
                    while (cursor.hasNext()) {
                        Record rec = cursor.next();
                        Assert.assertEquals(count, rec.getInt(0));

                        long len = rec.getBinLen(1);
                        DirectInputStream is = rec.getBin(1);
                        is.copyTo(addr, 0, len);

                        long p = addr;
                        int n = (rnd.nextPositiveInt() % (SZ - 1)) / 8;
                        for (int j = 0; j < n; j++) {
                            Assert.assertEquals(rnd.nextLong(), Unsafe.getUnsafe().getLong(p));
                            p += 8;
                        }
                        count++;
                    }
                } finally {
                    cursor.releaseCursor();
                }
            }
            Assert.assertEquals(N, count);
        } finally {
            ByteBuffers.release(buf);
        }
    }

    @Test
    public void testCreateAsSelectCastInconvertible() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try {
            compiler.createWriter(getFactory(), "create table x as (y order by t), cast(a as SYMBOL), cast(b as INT)");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(44, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectCastInconvertible2() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try {
            compiler.createWriter(getFactory(), "create table x as (y order by t), cast(h as INT), cast(b as INT)");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(44, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectCastMultipleWrong() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try {
            compiler.createWriter(getFactory(), "create table x as (y order by t), cast(a as LONG), cast(bz as INT)");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(56, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectCastWrongColumn() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try {
            compiler.createWriter(getFactory(), "create table x as (y order by t), cast(ab as LONG)");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(39, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectCastWrongType() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try {
            compiler.createWriter(getFactory(), "create table x as (y order by t), cast(a as LONGI)");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(44, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectIndexes() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try (JournalWriter w = compiler.createWriter(getFactory(), "create table x as (y order by t), index (a), index(x), index(z)")) {
            JournalMetadata m = w.getMetadata();
            Assert.assertEquals(11, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());

            Assert.assertEquals(ColumnType.SHORT, m.getColumn("c").getType());
            Assert.assertEquals(ColumnType.LONG, m.getColumn("d").getType());
            Assert.assertEquals(ColumnType.FLOAT, m.getColumn("e").getType());
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumn("f").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("g").getType());
            Assert.assertEquals(ColumnType.BINARY, m.getColumn("h").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertTrue(m.getColumn("x").isIndexed());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertTrue(m.getColumn("z").isIndexed());
            Assert.assertEquals(8, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.NONE, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateAsSelectLongIndex() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) record hint 100");
        exec("create table x as (y order by t), index(d) record hint 100");
    }

    @Test
    public void testCreateAsSelectPartitionBy() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try (JournalWriter w = compiler.createWriter(getFactory(), "create table x as (y order by t) partition by MONTH record hint 100")) {
            JournalMetadata m = w.getMetadata();
            Assert.assertEquals(11, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());

            Assert.assertEquals(ColumnType.SHORT, m.getColumn("c").getType());
            Assert.assertEquals(ColumnType.LONG, m.getColumn("d").getType());
            Assert.assertEquals(ColumnType.FLOAT, m.getColumn("e").getType());
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumn("f").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("g").getType());
            Assert.assertEquals(ColumnType.BINARY, m.getColumn("h").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertEquals(8, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateAsSelectPartitioned() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try (JournalWriter w = compiler.createWriter(getFactory(), "create table x as (y order by t) partition by MONTH record hint 100")) {
            JournalMetadata m = w.getMetadata();
            Assert.assertEquals(11, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());

            Assert.assertEquals(ColumnType.SHORT, m.getColumn("c").getType());
            Assert.assertEquals(ColumnType.LONG, m.getColumn("d").getType());
            Assert.assertEquals(ColumnType.FLOAT, m.getColumn("e").getType());
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumn("f").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("g").getType());
            Assert.assertEquals(ColumnType.BINARY, m.getColumn("h").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertEquals(8, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateAsSelectPartitionedMixedCase() throws Exception {
        exec("create table y (a INT, b byte, c Short, d long, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try (JournalWriter w = compiler.createWriter(getFactory(), "create table x as (y order by t) partition by MONTH record hint 100")) {
            JournalMetadata m = w.getMetadata();
            Assert.assertEquals(11, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());

            Assert.assertEquals(ColumnType.SHORT, m.getColumn("c").getType());
            Assert.assertEquals(ColumnType.LONG, m.getColumn("d").getType());
            Assert.assertEquals(ColumnType.FLOAT, m.getColumn("e").getType());
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumn("f").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("g").getType());
            Assert.assertEquals(ColumnType.BINARY, m.getColumn("h").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertEquals(8, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateAsSelectPartitionedNoTimestamp() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) record hint 100");
        try {
            exec("create table x as (y order by t) partition by MONTH record hint 100");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(46, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateAsSelectSymbolCount() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try (JournalWriter w = compiler.createWriter(getFactory(), "create table x as (y order by t), cast(x as SYMBOL count 33), cast(b as INT)")) {
            Assert.assertEquals(ColumnType.SYMBOL, w.getMetadata().getColumn("x").getType());
            Assert.assertEquals(63, w.getMetadata().getColumn("x").getBucketCount());
        }
    }

    @Test
    public void testCreateAsSelectSymbolCountError() throws Exception {
        exec("create table y (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t DATE, x SYMBOL, z STRING) timestamp(t) partition by YEAR record hint 100");
        try {
            exec("create table x as (y order by t), cast(x as SYMBOL 33), cast(b as INT)");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(51, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateDefaultPartitionBy() throws Exception {
        exec("create table x (a INT index, b BYTE, t DATE, z STRING index buckets 40, l LONG index buckets 500) record hint 100");
        // validate journal
        try (Journal r = getFactory().reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(5, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(1, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertTrue(m.getColumn("z").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(63, m.getColumn("z").getBucketCount());

            Assert.assertEquals(ColumnType.LONG, m.getColumn("l").getType());
            Assert.assertTrue(m.getColumn("l").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(511, m.getColumn("l").getBucketCount());

            Assert.assertEquals(-1, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.NONE, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateExistingTable() throws Exception {
        exec("create table x (a INT index buckets 25, b BYTE, t DATE, x SYMBOL index) timestamp(t) partition by MONTH");
        try {
            exec("create table x (a INT index buckets 25, b BYTE, t DATE, x SYMBOL index) timestamp(t) partition by MONTH");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(13, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateFromDefPartitionNoTimestamp() throws Exception {
        try {
            exec("create table x (a INT, b BYTE, x SYMBOL), index(a buckets 25), index(x) partition by YEAR record hint 100");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(85, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateIndexWithSuffix() throws Exception {
        exec("create table x (a INT, b BYTE, t DATE, x SYMBOL), index(a buckets 25), index(x) timestamp(t) partition by YEAR record hint 100");
        // validate journal
        try (Journal r = getFactory().reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(31, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertTrue(m.getColumn("x").isIndexed());

            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.YEAR, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateIndexWithSuffixDefaultPartition() throws Exception {
        exec("create table x (a INT, b BYTE, t DATE, x SYMBOL), index(a buckets 25), index(x) timestamp(t) record hint 100");
        // validate journal
        try (Journal r = getFactory().reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(31, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertTrue(m.getColumn("x").isIndexed());

            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.NONE, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateIndexedInt() throws Exception {
        exec("create table x (a INT index buckets 25, b BYTE, t DATE, x SYMBOL) timestamp(t) partition by MONTH record hint 100");
        // validate journal
        try (Journal r = getFactory().reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(31, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateIndexedIntDefaultBuckets() throws Exception {
        exec("create table x (a INT index, b BYTE, t DATE, x SYMBOL) timestamp(t) partition by MONTH record hint 100");
        // validate journal
        try (Journal r = getFactory().reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(1, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateIndexedString() throws Exception {
        exec("create table x (a INT index, b BYTE, t DATE, z STRING index buckets 40) timestamp(t) partition by MONTH record hint 100");
        // validate journal
        try (Journal r = getFactory().reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(1, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertTrue(m.getColumn("z").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(63, m.getColumn("z").getBucketCount());

            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateIndexedSymbol() throws Exception {
        exec("create table x (a INT index buckets 25, b BYTE, t DATE, x SYMBOL index) timestamp(t) partition by MONTH");
        // validate journal
        try (Journal r = getFactory().reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(31, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertTrue(m.getColumn("x").isIndexed());
            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateQuotedName() throws Exception {
        exec("create table 'a b' (a INT index, b BYTE, t DATE, z STRING index buckets 40, l LONG index buckets 500) record hint 100");
        // validate journal
        try (Journal r = getFactory().reader("a b")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(5, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(1, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertTrue(m.getColumn("z").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(63, m.getColumn("z").getBucketCount());

            Assert.assertEquals(ColumnType.LONG, m.getColumn("l").getType());
            Assert.assertTrue(m.getColumn("l").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(511, m.getColumn("l").getBucketCount());

            Assert.assertEquals(-1, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.NONE, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateReservedName() throws Exception {
        Files.mkDirsOrException(new File(getFactory().getConfiguration().getJournalBase(), "x"));
        try {
            exec("create table x (a INT, b BYTE, t DATE, x SYMBOL) partition by MONTH");
        } catch (ParserException e) {
            TestUtils.assertContains(QueryError.getMessage(), "reserved");
            Assert.assertEquals(13, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateSymbolWithCount1() throws Exception {
        try {
            exec("create table x (a INT, x SYMBOL count 20, z STRING, y BOOLEAN) timestamp(t) partition by MONTH record hint 100");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(32, QueryError.getPosition());
        }
    }

    @Test
    public void testInvalidPartitionBy() throws Exception {
        try {
            exec("create table x (a INT index, b BYTE, t DATE, z STRING index buckets 40) partition by x record hint 100");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(85, QueryError.getPosition());
        }
    }

    @Test
    public void testInvalidPartitionBy2() throws Exception {
        try {
            exec("create table x (a INT index, b BYTE, t DATE, z STRING index buckets 40) partition by 1 record hint 100");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(85, QueryError.getPosition());
        }
    }

    @Test
    public void testUnsupportedTypeForIndex() throws Exception {
        try {
            exec("create table x (a INT, b BYTE, t DATE, x SYMBOL), index(t) partition by YEAR");
            Assert.fail();
        } catch (ParserException ignored) {
            // pass
        }
    }

    private void exec(String ddl) throws JournalException, ParserException {
        compiler.execute(getFactory(), ddl);
    }
}
