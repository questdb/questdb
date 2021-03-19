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

import io.questdb.cutlass.pgwire.PGOids;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;

public class Constants {
    public static final String PUBLIC = "public";
    static final String[] NAMESPACES = {"pg_catalog", PUBLIC};
    static final StrFunction PUBLIC_CONSTANT = new StrConstant(0, PUBLIC);
    static final int[] NAMESPACE_OIDS = {PGOids.PG_CATALOG_OID, PGOids.PG_PUBLIC_OID};
    static final CharSequence[] KEYWORDS = {
            "add",
            "all",
            "alter",
            "and",
            "as",
            "asc",
            "asof",
            "backup",
            "between",
            "by",
            "cache",
            "capacity",
            "case",
            "cast",
            "column",
            "columns",
            "copy",
            "create",
            "cross",
            "database",
            "default",
            "delete",
            "desc",
            "distinct",
            "drop",
            "else",
            "end",
            "except",
            "exists",
            "fill",
            "foreign",
            "from",
            "grant",
            "group",
            "header",
            "if",
            "in",
            "index",
            "inner",
            "insert",
            "intersect",
            "into",
            "isolation",
            "join",
            "key",
            "latest",
            "left",
            "level",
            "limit",
            "lock",
            "lt",
            "nan",
            "natural",
            "nocache",
            "none",
            "not",
            "null",
            "on",
            "only",
            "or",
            "order",
            "outer",
            "over",
            "partition",
            "primary",
            "references",
            "rename",
            "repair",
            "right",
            "sample",
            "select",
            "show",
            "splice",
            "system",
            "table",
            "tables",
            "then",
            "to",
            "transaction",
            "truncate",
            "type",
            "union",
            "unlock",
            "update",
            "values",
            "when",
            "where",
            "with",
            "writer"
    };
}
