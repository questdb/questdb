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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.PropertyKey;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class ExportFilesFunctionFactoryTest extends AbstractCairoTest {
    private static String exportRoot;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        setupTestExportFiles();
    }

    @Test
    public void testExportFilesBasic() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "path\tdiskSize\tdiskSizeHuman\n" +
                            "analytics" + File.separator + "metrics.parquet\t496\t496.0 B\n" +
                            "analytics" + File.separator + "models" + File.separator + "prediction_model.parquet\t496\t496.0 B\n" +
                            "analytics" + File.separator + "results" + File.separator + "output.parquet\t496\t496.0 B\n" +
                            "exports" + File.separator + "data" + File.separator + "nested_table.parquet\t496\t496.0 B\n" +
                            "exports" + File.separator + "table1.parquet\t496\t496.0 B\n" +
                            "reports" + File.separator + "2023" + File.separator + "q1_report.parquet\t496\t496.0 B\n" +
                            "reports" + File.separator + "2024" + File.separator + "q2_summary.parquet\t496\t496.0 B\n" +
                            "reports" + File.separator + "monthly_report.csv\t7192\t7.0 KiB\n" +
                            "temp" + File.separator + "archived" + File.separator + "old_backup.parquet\t496\t496.0 B\n" +
                            "temp" + File.separator + "backup.sql\t1512\t1.5 KiB\n" +
                            "users_export.parquet\t496\t496.0 B\n" +
                            "users_export2.parquet\t496\t496.0 B\n",
                    "select path, diskSize, diskSizeHuman from export_files() order by path"
            );
        });
    }

    @Test
    public void testExportFilesDisabled() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, "");
            assertException(
                    "select * from export_files()",
                    14,
                    "export_files() is disabled ['cairo.sql.copy.export.root' is not set?]"
            );
        });
    }

    @Test
    public void testExportFilesEmptyDirectory() throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(exportRoot).$();
                if (ff.exists(path.$())) {
                    ff.rmdir(path);
                }
                ff.mkdir(path.$(), 493);
            }

            assertSql(
                    "path\tdiskSize\tdiskSizeHuman\tmodifiedTime\n",
                    "select * from export_files()"
            );
        });
    }

    private void createTestFile(String relativePath) {
        createTestFile(relativePath, getFileSize(relativePath));
    }

    private void createTestFile(String relativePath, int size) {
        try (Path path = new Path()) {
            path.of(exportRoot).concat(relativePath);
            long fd = Files.openRW(path.$());
            if (fd > -1) {
                try {
                    if (size > 0) {
                        long mem = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                        try {
                            for (int i = 0; i < size; i++) {
                                Unsafe.getUnsafe().putByte(mem + i, (byte) ('A' + (i % 26)));
                            }
                            Files.write(fd, mem, size, 0);
                        } finally {
                            Unsafe.free(mem, size, MemoryTag.NATIVE_DEFAULT);
                        }
                    }
                } finally {
                    Files.close(fd);
                }
            }
        }
    }

    private int getFileSize(String relativePath) {
        if (relativePath.endsWith(".csv")) return 7192;
        if (relativePath.endsWith(".parquet")) return 496;
        if (relativePath.endsWith(".sql")) return 1512;
        return 1024;
    }


    private void setupTestExportFiles() {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(exportRoot).$();
            if (ff.exists(path.$())) {
                ff.rmdir(path);
            }
            ff.mkdir(path.$(), 493);
            createTestFile("users_export.parquet");
            createTestFile("users_export2.parquet");
            ff.mkdir(path.of(exportRoot).concat("exports").$(), 493);
            ff.mkdir(path.of(exportRoot).concat("exports").concat("data").$(), 493);
            createTestFile("exports" + File.separator + "table1.parquet");
            createTestFile("exports" + File.separator + "data" + File.separator + "nested_table.parquet");
            ff.mkdir(path.of(exportRoot).concat("reports").$(), 493);
            ff.mkdir(path.of(exportRoot).concat("reports").concat("2023").$(), 493);
            ff.mkdir(path.of(exportRoot).concat("reports").concat("2024").$(), 493);
            createTestFile("reports" + File.separator + "monthly_report.csv");
            createTestFile("reports" + File.separator + "2023" + File.separator + "q1_report.parquet");
            createTestFile("reports" + File.separator + "2024" + File.separator + "q2_summary.parquet");
            ff.mkdir(path.of(exportRoot).concat("temp").$(), 493);
            ff.mkdir(path.of(exportRoot).concat("temp").concat("archived").$(), 493);
            createTestFile("temp" + File.separator + "backup.sql");
            createTestFile("temp" + File.separator + "archived" + File.separator + "old_backup.parquet");
            ff.mkdir(path.of(exportRoot).concat("analytics").$(), 493);
            ff.mkdir(path.of(exportRoot).concat("analytics").concat("models").$(), 493);
            ff.mkdir(path.of(exportRoot).concat("analytics").concat("results").$(), 493);
            createTestFile("analytics" + File.separator + "metrics.parquet");
            createTestFile("analytics" + File.separator + "models" + File.separator + "prediction_model.parquet");
            createTestFile("analytics" + File.separator + "results" + File.separator + "output.parquet");
        }
    }
}