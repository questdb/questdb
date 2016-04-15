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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.sort;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.ops.AbstractVirtualColumn;
import com.nfsdb.ql.parser.AbstractOptimiserTest;
import com.nfsdb.std.IntList;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ComparatorCompilerTest extends AbstractOptimiserTest {

    private final ComparatorCompiler cc = new ComparatorCompiler();

    @Test
    public void testAllGetters() throws Exception {
        JournalWriter w = factory.writer(new JournalStructure("xyz")
                .$bool("bool")
                .$byte("byte")
                .$double("double")
                .$float("float")
                .$int("int")
                .$long("long")
                .$date("date")
                .$short("short")
                .$str("str")
                .$sym("sym")
                .$());

        JournalEntryWriter ew = w.entryWriter();

        ew.putBool(0, true);
        ew.put(1, (byte) 13);
        ew.putDouble(2, 20.12);
        ew.putFloat(3, 10.15f);
        ew.putInt(4, 4);
        ew.putLong(5, 9988908080988890L);
        ew.putDate(6, 88979879L);
        ew.putShort(7, (short) 902);
        ew.putStr(8, "complexity made simple");
        ew.putSym(9, "nfsdb");
        ew.append();

        ew = w.entryWriter();
        ew.put(1, (byte) 13);
        ew.putDouble(2, 20.12);
        ew.putFloat(3, 10.15f);
        ew.putInt(4, 4);
        ew.putLong(5, 9988908080988890L);
        ew.putDate(6, 88979879L);
        ew.putShort(7, (short) 902);
        ew.putStr(8, "complexity made simple");
        ew.putSym(9, "appsicle");
        ew.append();

        w.commit();
        w.close();

        IntList indices = new IntList();
        for (int i = 0, n = w.getMetadata().getColumnCount(); i < n; i++) {
            indices.add(i + 1);
        }
        RecordSource rs = compiler.compileSource(factory, "xyz");
        RecordComparator rc = cc.compile(ComparatorCompilerTest.class, rs.getMetadata(), indices);
        RBTreeSortedRecordSource map = new RBTreeSortedRecordSource(rs, rc);

        sink.clear();
        printer.printCursor(map.prepareCursor(factory));
        TestUtils.assertEquals(
                "false\t13\t20.120000000000\t10.1500\t4\t9988908080988890\t1970-01-02T00:42:59.879Z\t902\tcomplexity made simple\tappsicle\n" +
                        "true\t13\t20.120000000000\t10.1500\t4\t9988908080988890\t1970-01-02T00:42:59.879Z\t902\tcomplexity made simple\tnfsdb\n",
                sink);
    }

    @Test
    public void testCompileAll() throws Exception {
        TestRecordMetadata m = new TestRecordMetadata().addDistinct();
        IntList indices = new IntList(m.getColumnCount());
        for (int i = 0, n = m.getColumnCount(); i < n; i++) {
            indices.add(i + 1);
        }
        RecordComparator rc = cc.compile(ComparatorCompilerTest.class, m, indices);
        Assert.assertNotNull(rc);
    }

    @Test
    public void testCompileLarge() throws Exception {
        TestRecordMetadata m = new TestRecordMetadata();
        for (int i = 0; i < 155; i++) {
            m.addDistinct();
        }
        IntList indices = new IntList(m.getColumnCount());
        for (int i = 0, n = m.getColumnCount(); i < n; i++) {
            indices.add(i + 1);
        }
        RecordComparator rc = cc.compile(ComparatorCompilerTest.class, m, indices);
        Assert.assertNotNull(rc);
    }

    @Test
    public void testCompileMultipleOfSame() throws Exception {
        TestRecordMetadata m = new TestRecordMetadata();
        for (int i = 0; i < 155; i++) {
            m.asType(ColumnType.STRING);
        }
        IntList indices = new IntList(m.getColumnCount());
        for (int i = 0, n = m.getColumnCount(); i < n; i++) {
            indices.add(i + 1);
        }
        RecordComparator rc = cc.compile(ComparatorCompilerTest.class, m, indices);
        Assert.assertNotNull(rc);
    }

    @Test
    public void testTwoClassesSameClassloader() throws Exception {
        TestRecordMetadata m = new TestRecordMetadata();
        for (int i = 0; i < 155; i++) {
            m.asType(ColumnType.STRING);
        }
        IntList indices = new IntList(m.getColumnCount());
        for (int i = 0, n = m.getColumnCount(); i < n; i++) {
            indices.add(i + 1);
        }
        RecordComparator rc1 = cc.compile(ComparatorCompilerTest.class, m, indices);
        RecordComparator rc2 = cc.compile(ComparatorCompilerTest.class, m, indices);

        Assert.assertNotNull(rc1);
        Assert.assertNotNull(rc2);
    }

    private static class TestColumnMetadata extends AbstractVirtualColumn {

        TestColumnMetadata(ColumnType type) {
            super(type);
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public void prepare(StorageFacade facade) {

        }
    }

    private static class TestRecordMetadata extends AbstractRecordMetadata {
        private final ObjList<TestColumnMetadata> columns = new ObjList<>();

        @Override
        public RecordColumnMetadata getColumn(int index) {
            return columns.get(index);
        }

        @Override
        public int getColumnCount() {
            return columns.size();
        }

        @Override
        public int getColumnIndexQuiet(CharSequence name) {
            return 0;
        }

        @Override
        public RecordColumnMetadata getColumnQuick(int index) {
            return columns.getQuick(index);
        }

        @Override
        public int getTimestampIndex() {
            return -1;
        }

        TestRecordMetadata addDistinct() {
            asType(ColumnType.BOOLEAN);
            asType(ColumnType.BYTE);
            asType(ColumnType.DOUBLE);
            asType(ColumnType.FLOAT);
            asType(ColumnType.INT);
            asType(ColumnType.LONG);
            asType(ColumnType.DATE);
            asType(ColumnType.SHORT);
            asType(ColumnType.STRING);
            asType(ColumnType.SYMBOL);
            return this;
        }

        void asType(ColumnType t) {
            columns.add(new TestColumnMetadata(t));
        }
    }
}