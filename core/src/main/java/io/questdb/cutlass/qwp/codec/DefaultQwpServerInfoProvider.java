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

package io.questdb.cutlass.qwp.codec;

/**
 * OSS-default {@link QwpServerInfoProvider}: reports the standalone role with a
 * fixed cluster id of {@code "questdb"} and the empty node id. Enterprise
 * substitutes its own provider via
 * {@code CairoConfiguration.getQwpServerInfoProvider()}.
 * <p>
 * The standalone cluster id is not empty so operators running multiple
 * independent OSS nodes can still set distinct ids via a future config key
 * without the client seeing two empty values as "same cluster".
 */
public final class DefaultQwpServerInfoProvider implements QwpServerInfoProvider {
    public static final DefaultQwpServerInfoProvider INSTANCE = new DefaultQwpServerInfoProvider();

    private DefaultQwpServerInfoProvider() {
    }

    @Override
    public int getCapabilities() {
        return 0;
    }

    @Override
    public CharSequence getClusterId() {
        return "questdb";
    }

    @Override
    public long getEpoch() {
        return 0L;
    }

    @Override
    public CharSequence getNodeId() {
        return "";
    }

    @Override
    public byte getRole() {
        return QwpEgressMsgKind.ROLE_STANDALONE;
    }
}
