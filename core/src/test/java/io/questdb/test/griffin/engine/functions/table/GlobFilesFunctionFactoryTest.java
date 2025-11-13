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

package io.questdb.test.griffin.engine.functions.table;

import io.questdb.griffin.engine.functions.table.GlobFilesFunctionFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GlobFilesFunctionFactoryTest {

    @Test
    public void testExtractNonGlobPrefixEmpty() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("");
        assertEquals("", result);
    }

    @Test
    public void testExtractNonGlobPrefixNoDirectoryPrefix() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("file*.txt");
        assertEquals("", result);
    }

    @Test
    public void testExtractNonGlobPrefixNoGlobPatterns() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("/path/to/dir");
        assertEquals("/path/to/dir", result);
    }

    @Test
    public void testExtractNonGlobPrefixNull() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix(null);
        assertEquals("", result);
    }

    @Test
    public void testExtractNonGlobPrefixSingleSlash() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("/");
        assertEquals("/", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithBackslash() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("path\\to\\data\\*.parquet");
        assertEquals("path\\to\\data", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithClosingBracketGlob() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("data/file].parquet");
        assertEquals("data", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithComplexPattern() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("/import/tables/2024/*/monthly/data_?.parquet");
        assertEquals("/import/tables/2024", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithGlobAtStart() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("*.parquet");
        assertEquals("", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithGlobInFileName() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("data/file[0-9].parquet");
        assertEquals("data", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithMixedSlashes() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("path/to\\data/file_*.parquet");
        assertEquals("path/to\\data", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithMultiLevelGlob() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("/path/to/data/*/file_?.csv");
        assertEquals("/path/to/data", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithQuestionMark() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("dir/file?.txt");
        assertEquals("dir", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithSingleLevelGlob() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("pattern/file_*.parquet");
        assertEquals("pattern", result);
    }

    @Test
    public void testExtractNonGlobPrefixWithTrailingGlob() {
        CharSequence result = GlobFilesFunctionFactory.extractNonGlobPrefix("/abs/path/to/file*.txt");
        assertEquals("/abs/path/to", result);
    }
}