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

package io.questdb.test.griffin.unionopt;

/**
 * One atom of a grant set. Grant-lattice tests enumerate every subset of a small list of atoms.
 */
public sealed interface Grant permits Grant.View, Grant.Columns {

    void applyTo(GrantPolicySecurityContext ctx);

    record View(String view) implements Grant {
        @Override
        public void applyTo(GrantPolicySecurityContext ctx) {
            ctx.grant(this);
        }
    }

    /**
     * SELECT on one column of a base table; {@code "*"} grants every column.
     */
    record Columns(String table, String column) implements Grant {
        @Override
        public void applyTo(GrantPolicySecurityContext ctx) {
            ctx.grant(this);
        }
    }
}
