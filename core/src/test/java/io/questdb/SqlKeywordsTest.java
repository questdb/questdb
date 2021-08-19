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

package io.questdb;

import io.questdb.griffin.SqlKeywords;
import org.junit.Assert;
import org.junit.Test;

public class SqlKeywordsTest {
    @Test
    public void testIsCharsGeoHashConstantValid() {
        Assert.assertTrue(SqlKeywords.isCharsGeoHashConstant("#0"));
        Assert.assertTrue(SqlKeywords.isCharsGeoHashConstant("#1"));
        Assert.assertTrue(SqlKeywords.isCharsGeoHashConstant("#sp"));
        Assert.assertTrue(SqlKeywords.isCharsGeoHashConstant("#sp052w92p1p8"));
    }

    @Test
    public void testIsCharsGeoHashConstantNotValid() {
        Assert.assertFalse(SqlKeywords.isCharsGeoHashConstant("0"));
        Assert.assertFalse(SqlKeywords.isCharsGeoHashConstant("##1"));
        Assert.assertFalse(SqlKeywords.isCharsGeoHashConstant("#sp@"));
        Assert.assertFalse(SqlKeywords.isCharsGeoHashConstant("#sp052w92p1p88"));
    }

    @Test
    public void testIsBitsGeoHashConstantValid() {
        Assert.assertTrue(SqlKeywords.isBitsGeoHashConstant("##0"));
        Assert.assertTrue(SqlKeywords.isBitsGeoHashConstant("##1"));
        Assert.assertTrue(SqlKeywords.isBitsGeoHashConstant("##111111111100000000001111111111000000000011111111110000000000"));
    }

    @Test
    public void testIsBitsGeoHashConstantNotValid() {
        Assert.assertFalse(SqlKeywords.isBitsGeoHashConstant("00110"));
        Assert.assertFalse(SqlKeywords.isBitsGeoHashConstant("#0"));
        Assert.assertFalse(SqlKeywords.isBitsGeoHashConstant("##"));
        Assert.assertFalse(SqlKeywords.isBitsGeoHashConstant("##12"));
        Assert.assertFalse(SqlKeywords.isBitsGeoHashConstant("##0111111111100000000001111111111000000000011111111110000000000"));
    }
}
