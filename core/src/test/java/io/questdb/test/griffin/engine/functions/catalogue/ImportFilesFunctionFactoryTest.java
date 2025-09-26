/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class ImportFilesFunctionFactoryTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        inputRoot = TestUtils.unchecked(() -> temp.newFolder("import").getAbsolutePath());
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, inputRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        setupTestFiles();
    }

    @Test
    public void testImportFilesBasic() throws Exception {
        assertMemoryLeak(() -> {
            assertSql(
                    "path\tsize\n" +
                            "analytics" + File.separator + "metrics.parquet\t0\n" +
                            "analytics" + File.separator + "models" + File.separator + "prediction_model.parquet\t0\n" +
                            "analytics" + File.separator + "results" + File.separator + "output.parquet\t0\n" +
                            "data" + File.separator + "file1.csv\t0\n" +
                            "data" + File.separator + "file2.parquet\t0\n" +
                            "data" + File.separator + "nested" + File.separator + "deep_file.parquet\t0\n" +
                            "nested" + File.separator + "deep" + File.separator + "file3.json\t0\n" +
                            "reports" + File.separator + "2023" + File.separator + "q1_report.parquet\t0\n" +
                            "reports" + File.separator + "2024" + File.separator + "q2_summary.parquet\t0\n" +
                            "reports" + File.separator + "monthly_report.csv\t0\n" +
                            "temp" + File.separator + "archived" + File.separator + "old_backup.parquet\t0\n" +
                            "temp" + File.separator + "backup.sql\t0\n" +
                            "test.txt\t0\n",
                    "select path, size from import_files() order by path"
            );
        });
    }

    @Test
    public void testImportFilesDisabled() throws Exception {
        assertMemoryLeak(() -> {
            String oldInputRoot = inputRoot;
            try {
                inputRoot = null;
                assertException(
                        "select * from import_files()",
                        14,
                        "import_files() is disabled ['cairo.sql.copy.root' is not set?]"
                );
            } finally {
                inputRoot = oldInputRoot;
            }

        });
    }

    @Test
    public void testImportFilesEmptyDirectory() throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(inputRoot).$();
                if (ff.exists(path.$())) {
                    ff.rmdir(path);
                }
                ff.mkdir(path.$(), 493);
            }

            assertSql(
                    "path\tsize\tmodified_time\n",
                    "select * from import_files()"
            );
        });
    }

    private void createTestFile(String relativePath) {
        try (Path path = new Path()) {
            path.of(inputRoot).concat(relativePath);
            Files.touch(path.$());
        }
    }

    private void setupTestFiles() {
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(inputRoot).$();
            if (ff.exists(path.$())) {
                ff.rmdir(path);
            }
            ff.mkdir(path.$(), 493);

            createTestFile("test.txt");

            ff.mkdir(path.of(inputRoot).concat("data").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("data").concat("nested").$(), 493);
            createTestFile("data" + File.separator + "file1.csv");
            createTestFile("data" + File.separator + "file2.parquet");
            createTestFile("data" + File.separator + "nested" + File.separator + "deep_file.parquet");

            ff.mkdir(path.of(inputRoot).concat("nested").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("nested").concat("deep").$(), 493);
            createTestFile("nested" + File.separator + "deep" + File.separator + "file3.json");

            ff.mkdir(path.of(inputRoot).concat("reports").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("reports").concat("2023").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("reports").concat("2024").$(), 493);
            createTestFile("reports" + File.separator + "monthly_report.csv");
            createTestFile("reports" + File.separator + "2023" + File.separator + "q1_report.parquet");
            createTestFile("reports" + File.separator + "2024" + File.separator + "q2_summary.parquet");

            ff.mkdir(path.of(inputRoot).concat("temp").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("temp").concat("archived").$(), 493);
            createTestFile("temp" + File.separator + "backup.sql");
            createTestFile("temp" + File.separator + "archived" + File.separator + "old_backup.parquet");

            ff.mkdir(path.of(inputRoot).concat("analytics").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("analytics").concat("models").$(), 493);
            ff.mkdir(path.of(inputRoot).concat("analytics").concat("results").$(), 493);
            createTestFile("analytics" + File.separator + "metrics.parquet");
            createTestFile("analytics" + File.separator + "models" + File.separator + "prediction_model.parquet");
            createTestFile("analytics" + File.separator + "results" + File.separator + "output.parquet");
        }
    }
}