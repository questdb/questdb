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

package io.questdb.griffin.engine.ops;

import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.IQueryModel;

/**
 * The execution model {@code CREATE LIVE VIEW} parses into, and the seam an edition can wrap.
 * {@link io.questdb.griffin.SqlParserCallback#parseCreateLiveViewExt} hands the parsed
 * {@link CreateLiveViewOperationBuilderImpl} to the callback and takes whatever implementation of
 * this interface it returns, so an edition that extends the grammar (Enterprise appends
 * {@code OWNED BY '<principal>'}) can decorate the operation the builder produces without the
 * parser knowing about the extension. Mirrors {@link CreateViewOperationBuilder} and
 * {@link CreateMatViewOperationBuilder}.
 */
public interface CreateLiveViewOperationBuilder extends ExecutionModel {

    CreateLiveViewOperation build(CharSequence sqlText);

    @Override
    default int getModelType() {
        return CREATE_LIVE_VIEW;
    }

    void setSelectModel(IQueryModel selectModel);
}
