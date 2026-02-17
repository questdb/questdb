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

package io.questdb.test.griffin.engine.functions.regex;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class GlobStrFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testGlobBracketExpression() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file_a.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_b.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_c.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_x.txt' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            file_a.txt
                            file_b.txt
                            file_c.txt
                            """,
                    "select * from x where glob(name, 'file_[abc].txt')"
            );
        });
    }

    @Test
    public void testGlobBracketNegation() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file_a.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_b.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_c.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_x.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_1.txt' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            file_x.txt
                            file_1.txt
                            """,
                    "select * from x where glob(name, 'file_[!abc].txt')"
            );
        });
    }

    @Test
    public void testGlobBracketRange() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file_0.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_5.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_9.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_a.txt' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            file_0.txt
                            file_5.txt
                            file_9.txt
                            """,
                    "select * from x where glob(name, 'file_[0-9].txt')"
            );
        });
    }

    @Test
    public void testGlobComplexBracketExpression() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('data_0.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_5.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_9.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_a.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_z.csv' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            data_0.csv
                            data_5.csv
                            data_9.csv
                            """,
                    "select * from x where glob(name, 'data_[0-9].csv')"
            );
        });
    }

    @Test
    public void testGlobComplexBracketWithNegation() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('data_0.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_a.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_A.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_Z.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_z.csv' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            data_0.csv
                            """,
                    "select * from x where glob(name, 'data_[!a-zA-Z].csv')"
            );
        });
    }

    // Nested patterns
    @Test
    public void testGlobDoubleStar() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file.txt' as string) as path from long_sequence(1)
                    union
                    select cast('dir/file.txt' as string) as path from long_sequence(1)
                    union
                    select cast('dir/subdir/file.txt' as string) as path from long_sequence(1)
                    union
                    select cast('other.csv' as string) as path from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            path
                            dir/file.txt
                            dir/subdir/file.txt
                            """,
                    "select * from x where glob(path, '**/file.txt')"
            );
        });
    }

    // Edge cases
    @Test
    public void testGlobExactMatch() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file.txt' as string) as name from long_sequence(1)
                    union
                    select cast('readme.txt' as string) as name from long_sequence(1)
                    union
                    select cast('' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            file.txt
                            """,
                    "select * from x where glob(name, 'file.txt')"
            );
        });
    }

    @Test
    public void testGlobMultipleWildcards() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('log_2024_01_app.txt' as string) as name from long_sequence(1)
                    union
                    select cast('log_2024_02_app.txt' as string) as name from long_sequence(1)
                    union
                    select cast('log_2023_01_app.txt' as string) as name from long_sequence(1)
                    union
                    select cast('data_2024_01.txt' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            log_2024_01_app.txt
                            log_2024_02_app.txt
                            """,
                    "select * from x where glob(name, 'log_2024_*_app.txt')"
            );
        });
    }

    @Test
    public void testGlobNegationOutsideBracket() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file!test.txt' as string) as name from long_sequence(1)
                    union
                    select cast('fileXtest.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file!.txt' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            file!test.txt
                            file!.txt
                            """,
                    "select * from x where glob(name, 'file!*')"
            );
        });
    }

    @Test
    public void testGlobNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file.txt' as string) as name from long_sequence(1)
                    union
                    select cast('readme.pdf' as string) as name from long_sequence(1)
                    union
                    select cast('document.doc' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    "name\n",
                    "select * from x where glob(name, '*.csv')"
            );
        });
    }

    @Test
    public void testGlobOnlyAsterisk() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file.txt' as string) as name from long_sequence(1)
                    union
                    select cast('readme' as string) as name from long_sequence(1)
                    union
                    select cast('' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            file.txt
                            readme
                            
                            """,
                    "select * from x where glob(name, '*')"
            );
        });
    }

    // Basic glob patterns tests
    @Test
    public void testGlobSimpleAsterisk() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file.txt' as string) as name from long_sequence(1)
                    union
                    select cast('readme.txt' as string) as name from long_sequence(1)
                    union
                    select cast('document.pdf' as string) as name from long_sequence(1)
                    union
                    select cast('image.jpg' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            file.txt
                            readme.txt
                            """,
                    "select * from x where glob(name, '*.txt')"
            );
        });
    }

    @Test
    public void testGlobSimpleQuestionMark() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file1.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file2.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file12.txt' as string) as name from long_sequence(1)
                    union
                    select cast('readme.txt' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            file1.txt
                            file2.txt
                            """,
                    "select * from x where glob(name, 'file?.txt')"
            );
        });
    }

    // Unicode and whitespace
    @Test
    public void testGlobUnicodeCharacters() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('файл_2024.txt' as string) as name from long_sequence(1)
                    union
                    select cast('файл_2023.txt' as string) as name from long_sequence(1)
                    union
                    select cast('document_2024.txt' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            файл_2024.txt
                            файл_2023.txt
                            """,
                    "select * from x where glob(name, 'файл_*.txt')"
            );
        });
    }

    // Case sensitivity and special characters
    @Test
    public void testGlobWithDot() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('file.name.txt' as string) as name from long_sequence(1)
                    union
                    select cast('filename.txt' as string) as name from long_sequence(1)
                    union
                    select cast('file_name.txt' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            file.name.txt
                            """,
                    "select * from x where glob(name, 'file.name.txt')"
            );
        });
    }

    @Test
    public void testGlobWithNumbers() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('data_2024_01.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_2023_01.csv' as string) as name from long_sequence(1)
                    union
                    select cast('data_2024_02.csv' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            data_2024_01.csv
                            data_2024_02.csv
                            """,
                    "select * from x where glob(name, 'data_2024_*.csv')"
            );
        });
    }

    @Test
    public void testGlobWithPath() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('/var/log/app_2024.log' as string) as path from long_sequence(1)
                    union
                    select cast('/var/log/app_2023.log' as string) as path from long_sequence(1)
                    union
                    select cast('/var/data/app.log' as string) as path from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            path
                            /var/log/app_2024.log
                            /var/log/app_2023.log
                            """,
                    "select * from x where glob(path, '/var/log/app_*.log')"
            );
        });
    }

    @Test
    public void testGlobWithWhitespace() throws Exception {
        assertMemoryLeak(() -> {
            String sql = """
                    create table x as (
                    select cast('my file 2024.txt' as string) as name from long_sequence(1)
                    union
                    select cast('my file 2023.txt' as string) as name from long_sequence(1)
                    union
                    select cast('other_file.txt' as string) as name from long_sequence(1)
                    )""";
            execute(sql);
            assertSql(
                    """
                            name
                            my file 2024.txt
                            my file 2023.txt
                            """,
                    "select * from x where glob(name, 'my file *.txt')"
            );
        });
    }
}
