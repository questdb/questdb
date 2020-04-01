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

package io.questdb.griffin;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoSecurityContext;
import io.questdb.griffin.engine.functions.bind.BindVariableService;
import io.questdb.std.IntStack;
import org.jetbrains.annotations.Nullable;

public class SqlExecutionContextImpl implements SqlExecutionContext {
    private final IntStack timestampRequiredStack = new IntStack();
    private BindVariableService bindVariableService;
    private CairoSecurityContext cairoSecurityContext;
    @Nullable
    private MessageBus messageBus;

    @Override
    public BindVariableService getBindVariableService() {
        return bindVariableService;
    }

    @Override
    public CairoSecurityContext getCairoSecurityContext() {
        return cairoSecurityContext;
    }

    @Override
    @Nullable
    public MessageBus getMessageBus() {
        return messageBus;
    }

    @Override
    public boolean isTimestampRequired() {
        return timestampRequiredStack.notEmpty() && timestampRequiredStack.peek() == 1;
    }

    @Override
    public void popTimestampRequiredFlag() {
        timestampRequiredStack.pop();
    }

    @Override
    public void pushTimestampRequiredFlag(boolean flag) {
        timestampRequiredStack.push(flag ? 1 : 0);
    }

    public SqlExecutionContextImpl with(
            CairoSecurityContext cairoSecurityContext,
            BindVariableService bindVariableService,
            @Nullable MessageBus messageBus
    ) {
        this.cairoSecurityContext = cairoSecurityContext;
        this.bindVariableService = bindVariableService;
        this.messageBus = messageBus;
        return this;
    }
}
