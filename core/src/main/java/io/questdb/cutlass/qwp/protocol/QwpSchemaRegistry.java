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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.ObjList;

/**
 * Registry for QWP v1 schemas, keyed by client-assigned schema ID.
 * <p>
 * Schema IDs are dense, monotonically increasing integers starting at 0,
 * so the registry uses a plain list where the index is the schema ID.
 * Single-threaded, zero-allocation on lookups.
 */
public class QwpSchemaRegistry {

    private final int maxSchemasPerConnection;
    private final ObjList<QwpSchema> schemas = new ObjList<>();
    private long hits;
    private long misses;

    public QwpSchemaRegistry() {
        this(QwpConstants.DEFAULT_MAX_SCHEMAS_PER_CONNECTION);
    }

    public QwpSchemaRegistry(int maxSchemasPerConnection) {
        this.maxSchemasPerConnection = maxSchemasPerConnection;
    }

    public void clear() {
        schemas.clear();
        hits = 0;
        misses = 0;
    }

    public QwpSchema get(int schemaId) {
        if (schemaId >= 0 && schemaId < schemas.size()) {
            QwpSchema schema = schemas.getQuick(schemaId);
            if (schema != null) {
                hits++;
                return schema;
            }
        }
        misses++;
        return null;
    }

    public double getHitRate() {
        long total = hits + misses;
        return total > 0 ? (double) hits / total : 0.0;
    }

    public long getHits() {
        return hits;
    }

    public long getMisses() {
        return misses;
    }

    public void put(int schemaId, QwpSchema schema) throws QwpParseException {
        if (schemaId < 0 || schemaId >= maxSchemasPerConnection) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_SCHEMA_ID,
                    "schema ID out of range [schemaId=" + schemaId
                            + ", maxSchemasPerConnection=" + maxSchemasPerConnection + ']'
            );
        }
        // Accept re-registration of already-known schemas. The client may
        // re-send a full schema when its confirmed-schema tracking lags,
        // or when tables are encoded in hash map iteration order (not
        // schema ID assignment order).
        schemas.extendAndSet(schemaId, schema);
    }

    public int size() {
        return schemas.size();
    }
}
