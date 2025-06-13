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

package io.questdb.cairo.view;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

public class ViewDefinition implements Mutable {
    public static final String VIEW_DEFINITION_FILE_NAME = "_view";
    public static final int VIEW_DEFINITION_FORMAT_MSG_TYPE = 0;
    private final ObjList<CharSequence> dependencies = new ObjList<>();
    private String viewSql;
    private TableToken viewToken;

    public static void append(@NotNull ViewDefinition viewDefinition, @NotNull AppendableBlock block) {
        block.putStr(viewDefinition.getViewSql());
    }

    public static void append(@NotNull ViewDefinition viewDefinition, @NotNull BlockFileWriter writer) {
        final AppendableBlock block = writer.append();
        append(viewDefinition, block);
        block.commit(VIEW_DEFINITION_FORMAT_MSG_TYPE);
        writer.commit();
    }

    public static void readFrom(
            @NotNull ViewDefinition destDefinition,
            @NotNull BlockFileReader reader,
            @NotNull Path path,
            int rootLen,
            @NotNull TableToken viewToken
    ) {
        path.trimTo(rootLen).concat(viewToken.getDirName()).concat(VIEW_DEFINITION_FILE_NAME);
        reader.of(path.$());
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == VIEW_DEFINITION_FORMAT_MSG_TYPE) {
                readDefinitionBlock(destDefinition, block, viewToken);
                return;
            }
        }
        throw CairoException.critical(0)
                .put("cannot read view definition, block not found [path=").put(path)
                .put(']');
    }

    @Override
    public void clear() {
        viewToken = null;
        viewSql = null;
        dependencies.clear();
    }

    public ObjList<CharSequence> getDependencies() {
        return dependencies;
    }

    public String getViewSql() {
        return viewSql;
    }

    public TableToken getViewToken() {
        return viewToken;
    }

    public void init(
            @NotNull TableToken viewToken,
            @NotNull String viewSql
    ) {
        this.viewToken = viewToken;
        this.viewSql = viewSql;
    }

    private static void readDefinitionBlock(
            ViewDefinition destDefinition,
            ReadableBlock block,
            TableToken viewToken
    ) {
        assert block.type() == VIEW_DEFINITION_FORMAT_MSG_TYPE;

        long offset = 0;

        final CharSequence viewSql = block.getStr(offset);
        if (viewSql == null || viewSql.length() == 0) {
            throw CairoException.critical(0)
                    .put("view SQL is empty [view=")
                    .put(viewToken.getTableName())
                    .put(']');
        }
        final String viewSqlStr = Chars.toString(viewSql);

        destDefinition.init(
                viewToken,
                viewSqlStr
        );
    }
}
