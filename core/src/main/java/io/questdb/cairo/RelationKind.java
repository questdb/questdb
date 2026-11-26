/*+*****************************************************************************
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


package io.questdb.cairo;

/**
 * The class of values a type belongs to, as {@link TypeDriver#getRelationKind()} returns it. {@link
 * RelationRules} groups types by kind, so its rules need not list a type by tag. UNDEF, PSEUDO and
 * NULL are the kinds RelationRules assigns to the pseudo tags, which have no type driver.
 */
public enum RelationKind {
    UNDEF,
    BOOL,
    INT,
    CHAR,
    FLOAT,
    TEMPORAL,
    TEXT,
    SYMBOL,
    LONG256,
    LONG128,
    UUID,
    IPV4,
    BINARY,
    GEO,
    DECIMAL,
    ARRAY,
    INTERVAL,
    PSEUDO,
    NULL
}
