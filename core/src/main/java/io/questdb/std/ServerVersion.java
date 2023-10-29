/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.std;

import io.questdb.BuildInformationHolder;
import io.questdb.griffin.engine.functions.catalogue.ShowServerVersionCursorFactory;

public class ServerVersion {

    public static final String SERVER_VERSION;

    static {
        BuildInformationHolder buildInformation = new BuildInformationHolder(ShowServerVersionCursorFactory.class);
        StringBuilder sb = new StringBuilder();
        sb.append(buildInformation.getSwName()).append(' ')
                .append(buildInformation.getSwVersion()).append(" JDK ")
                .append(buildInformation.getJdkVersion()).append(", Hash ")
                .append(buildInformation.getCommitHash()).append(" (");

        switch (Os.type) {
            case Os.FREEBSD:
                sb.append("OS/ARCH freebsd/amd64");
                break;
            case Os.LINUX_AMD64:
                sb.append("OS/Arch linux/amd64");
                break;
            case Os.LINUX_ARM64:
                sb.append("OS/Arch linux/arm64");
                break;
            case Os.OSX_AMD64:
                sb.append("OS/Arch apple/amd64");
                break;
            case Os.OSX_ARM64:
                sb.append("OS/Arch apple/apple-silicon");
                break;
            case Os.WINDOWS:
                sb.append("OS/Arch Windows/amd64");
                break;
            default:
                sb.append("Unsupported OS");
                break;
        }
        sb.append(Vect.getSupportedInstructionSetName());
        if (sb.charAt(sb.length() - 1) == ']') {
            sb.setLength(sb.length() - 1);
        }
        sb.append(", ").append(System.getProperty("os.arch")).append(", ")
                .append(Runtime.getRuntime().availableProcessors()).append(" CPU")
                .append("])");
        SERVER_VERSION = sb.toString();
    }

}
