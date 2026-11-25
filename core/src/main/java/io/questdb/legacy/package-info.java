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

/**
 * The query planner as it was at commit {@code 94874b31a225712690e8e711305fc8c9cc4e41a4}, kept as a fallback
 * while the logical-plan planner rolls out. The {@code cairo.sql.legacy.planner.enabled} property, the
 * {@code Legacy-Planner} HTTP header and the {@code legacy_planner} PostgreSQL startup option select it.
 * <p>
 * {@code ci/legacy_planner.py} generates these classes from that commit. The legacy planner takes no fixes, and
 * goes once the new planner has shipped.
 */
package io.questdb.legacy;
