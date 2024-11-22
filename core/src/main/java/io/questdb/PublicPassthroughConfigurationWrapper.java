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

package io.questdb;

import io.questdb.std.CharSequenceObjHashMap;

public class PublicPassthroughConfigurationWrapper implements PublicPassthroughConfiguration {
    private final PublicPassthroughConfiguration delegate;

    protected PublicPassthroughConfigurationWrapper() {
        delegate = null;
    }

    @Override
    public String getPosthogApiKey() {
        return getDelegate().getPosthogApiKey();
    }

    @Override
    public boolean isPosthogEnabled() {
        return getDelegate().isPosthogEnabled();
    }

    @Override
    public void populateSettings(CharSequenceObjHashMap<CharSequence> settings) {
        getDelegate().populateSettings(settings);
    }

    protected PublicPassthroughConfiguration getDelegate() {
        return delegate;
    }
}
