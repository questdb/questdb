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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import org.junit.Test;

public class BrokenIntReadTest extends AbstractGriffinTest {


    @Test
    public void testFailToReadInt_TableIdOfFirstTable() throws Exception {
        String expected = "adrelid\tadnum\tadbin\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n";

        testFailOnRead(1, expected);
    }


    @Test
    public void testFailToReadInt_ColumnCountOfFirstTable() throws Exception {
        String expected = "adrelid\tadnum\tadbin\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n";

        testFailOnRead(2, expected);
    }

    @Test
    public void testFailToReadInt_TableIdOfSecondTable() throws Exception {
        String expected = "adrelid\tadnum\tadbin\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n";

        testFailOnRead(3, expected);
    }


    @Test
    public void testFailToReadInt_ColumnCountOfSecondTable() throws Exception {
        String expected = "adrelid\tadnum\tadbin\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n";

        testFailOnRead(4, expected);
    }

    @Test
    public void testFailToReadInt_TableIdOfThirdTable() throws Exception {
        String expected = "adrelid\tadnum\tadbin\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n";

        testFailOnRead(5, expected);
    }

    @Test
    public void testFailToReadInt_ColumnCountOfThirdTable() throws Exception {
        String expected = "adrelid\tadnum\tadbin\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n" +
                "2\t1\t\n" +
                "2\t2\t\n" +
                "2\t3\t\n" +
                "2\t4\t\n" +
                "2\t5\t\n";

        testFailOnRead(6, expected);
    }

    @Test
    public void testFailToReadIntDescriptionFunc_TableIdOfFirstTable() throws Exception {
        String expected = "objoid\tclassoid\tobjsubid\tdescription\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "11\t2615\t0\tdescription\n" +
                "2200\t2615\t0\tdescription\n";

        testFailOnReadDescriptionFunc(1, expected);
    }


    @Test
    public void testFailToReadIntDescriptionFunc_ColumnCountOfFirstTable() throws Exception {
        String expected = "objoid\tclassoid\tobjsubid\tdescription\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "11\t2615\t0\tdescription\n" +
                "2200\t2615\t0\tdescription\n";

        testFailOnReadDescriptionFunc(2, expected);
    }

    @Test
    public void testFailToReadIntDescriptionFunc_TableIdOfSecondTable() throws Exception {
        String expected = "objoid\tclassoid\tobjsubid\tdescription\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "11\t2615\t0\tdescription\n" +
                "2200\t2615\t0\tdescription\n";

        testFailOnReadDescriptionFunc(3, expected);
    }


    @Test
    public void testFailToReadIntDescriptionFunc_ColumnCountOfSecondTable() throws Exception {
        String expected = "objoid\tclassoid\tobjsubid\tdescription\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "11\t2615\t0\tdescription\n" +
                "2200\t2615\t0\tdescription\n";

        testFailOnReadDescriptionFunc(4, expected);
    }

    @Test
    public void testFailToReadIntDescriptionFunc_TableIdOfThirdTable() throws Exception {
        String expected = "objoid\tclassoid\tobjsubid\tdescription\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "11\t2615\t0\tdescription\n" +
                "2200\t2615\t0\tdescription\n";

        testFailOnReadDescriptionFunc(5, expected);
    }

    @Test
    public void testFailToReadIntDescriptionFunc_ColumnCountOfThirdTable() throws Exception {
        String expected = "objoid\tclassoid\tobjsubid\tdescription\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "2\t1259\t0\ttable\n" +
                "2\t1259\t1\tcolumn\n" +
                "2\t1259\t2\tcolumn\n" +
                "2\t1259\t3\tcolumn\n" +
                "2\t1259\t4\tcolumn\n" +
                "2\t1259\t5\tcolumn\n" +
                "11\t2615\t0\tdescription\n" +
                "2200\t2615\t0\tdescription\n";

        testFailOnReadDescriptionFunc(6, expected);
    }

    private void testFailOnRead(int i, String expected) throws Exception {
        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return new BrokenIntRead(i);
            }
        };
        engine = new CairoEngine(configuration);
        compiler = new SqlCompiler(engine);
        sqlExecutionContext = new SqlExecutionContextImpl(
                engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);

        createTables(configuration.getFilesFacade());

        assertQuery(
                expected,
                "pg_catalog.pg_attrdef order by 1",
                null,
                null,
                true,
                false,
                false
        );
    }

    private void testFailOnReadDescriptionFunc(int i, String expected) throws Exception {


        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return new BrokenIntRead(i);
            }
        };
        engine = new CairoEngine(configuration);
        compiler = new SqlCompiler(engine);
        sqlExecutionContext = new SqlExecutionContextImpl(
                engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);

        createTables(configuration.getFilesFacade());

        assertQuery(
                expected,
                "pg_catalog.pg_description;",
                null,
                null,
                false,
                false,
                false
        );
    }

    private void createTables(FilesFacade ff) {
        try (TableModel model = new TableModel(new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        }, "x", PartitionBy.NONE)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("supplier", ColumnType.SYMBOL)
                .col("category", ColumnType.SYMBOL)
                .timestamp()) {
            CairoTestUtils.createTableWithVersionAndId(model, ColumnType.VERSION, 2);
        }

        try (TableModel model = new TableModel(new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        }, "y", PartitionBy.NONE)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("supplier", ColumnType.SYMBOL)
                .col("category", ColumnType.SYMBOL)

                .timestamp()) {
            CairoTestUtils.createTableWithVersionAndId(model, ColumnType.VERSION, 2);
        }

        try (TableModel model = new TableModel(new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        }, "z", PartitionBy.NONE)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("supplier", ColumnType.SYMBOL)
                .col("category", ColumnType.SYMBOL)
                .timestamp()) {
            CairoTestUtils.createTableWithVersionAndId(model, ColumnType.VERSION, 2);
        }
    }

    static class BrokenIntRead extends FilesFacadeImpl {

        private final int failOnCount;
        private int callCount = 0;

        public BrokenIntRead(int failOnCount) {
            this.failOnCount = failOnCount;
        }

        @Override
        public long read(long fd, long buf, long len, long offset) {
            if (fd > 18) {
                callCount++;
            }
            if (callCount == failOnCount) {
                return -1;
            }
            return super.read(fd, buf, len, offset);
        }

        @Override
        public void findClose(long findPtr) {
            callCount = 0;
            super.findClose(findPtr);
        }
    }
}
