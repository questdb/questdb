/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.thrift;

import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.factory.NullsAdaptor;
import com.nfsdb.journal.factory.NullsAdaptorFactory;
import org.apache.thrift.TBase;

public class ThriftNullsAdaptorFactory implements NullsAdaptorFactory<TBase> {

    @Override
    public <T extends TBase> NullsAdaptor<T> getInstance(Class<T> type) throws JournalConfigurationException {
        return new ThriftNullsAdaptor<>(type);
    }
}
