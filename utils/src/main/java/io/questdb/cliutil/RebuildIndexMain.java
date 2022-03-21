/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cliutil;


import io.questdb.BuildInformation;
import io.questdb.BuildInformationHolder;
import io.questdb.PropServerConfiguration;
import io.questdb.ServerConfigurationException;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RebuildIndex;
import io.questdb.cutlass.json.JsonException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class RebuildIndexMain {
    public static void main(String[] args) throws IOException, JsonException, ServerConfigurationException {
        LogFactory.configureSync();

        CommandLineArgs params = parseCommandArgs(args);
        if (params == null) {
            // Invalid params, usage already printed
            return;
        }

        RebuildIndex ri = new RebuildIndex();
        String rootDirectory = params.tablePath + Files.SEPARATOR + ".." + Files.SEPARATOR + "..";
        final Properties properties = new Properties();
        final String configurationFileName = "/server.conf";
        final File configurationFile = new File(new File(rootDirectory, PropServerConfiguration.CONFIG_DIRECTORY), configurationFileName);

        try (InputStream is = new FileInputStream(configurationFile)) {
            properties.load(is);
        }
        final Log log = LogFactory.getLog("rebuild-index");
        PropServerConfiguration configuration = readServerConfiguration(rootDirectory, properties, log, new BuildInformationHolder());

        ri.of(params.tablePath, configuration.getCairoConfiguration());
        try {
            ri.rebuildPartitionColumn(params.partition, params.column);
        } catch (CairoException ex) {
            log.error().$(ex.getFlyweightMessage()).$();
        }
    }

    static CommandLineArgs parseCommandArgs(String[] args) {

        if (args.length > 5 || args.length % 2 != 1) {
            printUsage();
            return null;
        }

        CommandLineArgs res = new CommandLineArgs();
        res.tablePath = args[0];
        for (int i = 1, n = args.length; i < n; i += 2) {
            if ("-p".equals(args[i])) {
                if (res.partition == null) {
                    res.partition = args[i + 1];
                } else {
                    System.err.println("-p parameter can be only used once");
                    printUsage();
                    return null;
                }
            }

            if ("-c".equals(args[i])) {
                if (res.column == null) {
                    res.column = args[i + 1];
                } else {
                    System.err.println("-c parameter can be only used once");
                    printUsage();
                    return null;
                }
            }
        }

        if (res.tablePath.endsWith(String.valueOf(Files.SEPARATOR))) {
            res.tablePath = res.tablePath.substring(res.tablePath.length());
        }

        return res;
    }

    private static void printUsage() {
        System.out.println("usage: " + RebuildIndexMain.class.getName() + " <table_path> [-p <partition_name>] [-c <column_name>]");
    }

    private static PropServerConfiguration readServerConfiguration(
            final String rootDirectory,
            final Properties properties,
            Log log,
            final BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
        return new PropServerConfiguration(rootDirectory, properties, System.getenv(), log, buildInformation);
    }

    static class CommandLineArgs {
        String tablePath;
        String partition;
        String column;
    }
}
