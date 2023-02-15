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

package io.questdb.cliutil;

import io.questdb.client.Sender;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.Timestamps;

import java.sql.*;
import java.util.Properties;

public class Table2IlpCopier {

    public long copyTable(Table2Ilp.Table2IlpParams params) {
        LowerCaseCharSequenceHashSet symbols = createSymbolsSet(params);

        MicrosecondClock microsecondClock = new MicrosecondClockImpl();
        long totalSentLines = 0;
        try (Connection connection = getConnection(params.getSourcePgConnectionString())) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(params.getSourceSelectQuery())) {
                statement.setFetchSize(8 * 1024);
                long start = microsecondClock.getTicks();

                try (ResultSet resultSet = statement.executeQuery()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    String[] columnNames = new String[columnCount];
                    int[] columnTypes = new int[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        columnNames[i] = metaData.getColumnName(i + 1);
                        columnTypes[i] = metaData.getColumnType(i + 1);
                    }
                    int timestampIndex = getTimestampIndex(columnNames, columnTypes, params.getSourceTimestampColumnName());

                    try (Sender sender = buildLineTcpSender(params)) {
                        String tableName = params.getDestinationTableName();

                        try {
                            while (resultSet.next()) {
                                sendLine(symbols, resultSet, columnCount, columnNames, columnTypes, timestampIndex, sender, tableName);
                                totalSentLines++;

                                if (totalSentLines % 10000 == 0) {
                                    long end = microsecondClock.getTicks();
                                    long linesPerSec = 10000 * Timestamps.SECOND_MICROS / (end - start);
                                    System.out.println(totalSentLines + " lines, " + linesPerSec + " lines/sec");
                                    start = microsecondClock.getTicks();
                                }
                            }
                        } catch (Exception th) {
                            try {
                                sender.flush();
                            } catch (Exception flushError) {
                                System.err.println("Failed to flush sender while handling error");
                                flushError.printStackTrace();
                            }
                            throw th;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("Failed to connect to the source");
        } finally {
            System.out.println("Total sent lines: " + totalSentLines);
        }
        return totalSentLines;
    }

    private static Sender buildLineTcpSender(Table2Ilp.Table2IlpParams params) {
        Sender.LineSenderBuilder senderBuilder = Sender.builder();
        senderBuilder.address(params.getDestinationIlpHost() + ":" + params.getDestinationIlpPort());
        if (params.enableDestinationTls()) {
            senderBuilder.enableTls();
        }

        if (params.getDestinationAuthKey() != null) {
            senderBuilder
                    .enableAuth(params.getDestinationAuthKey())
                    .authToken(params.getDestinationAuthToken());
        }
        return senderBuilder.build();
    }

    private static LowerCaseCharSequenceHashSet createSymbolsSet(Table2Ilp.Table2IlpParams params) {
        LowerCaseCharSequenceHashSet set = new LowerCaseCharSequenceHashSet();
        String[] symbols = params.getSymbols();
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, n = symbols.length; i < n; i++) {
            set.add(symbols[i]);
        }
        return set;
    }

    private static Connection getConnection(String connectionString) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", "true");
        properties.setProperty("preferQueryMode", "extendedForPrepared");
        properties.setProperty("options", "-c statement_timeout=" + Integer.MAX_VALUE);
        return DriverManager.getConnection(connectionString, properties);
    }

    private static long getMicroEpoch(ResultSet resultSet, int timestampIndex) throws SQLException {
        String ts = resultSet.getString(timestampIndex);
        long microEpoch;
        if (ts != null) {
            try {
                microEpoch = IntervalUtils.parseFloorPartialTimestamp(ts);
            } catch (NumericException e) {
                throw new RuntimeException("Failed to parse designated timestamp: " + ts);
            }
            return microEpoch;
        }
        return Numbers.LONG_NaN;
    }

    private static int getTimestampIndex(String[] columnNames, int[] columnTypes, String sourceTimestampColumnName) {
        sourceTimestampColumnName = sourceTimestampColumnName.toLowerCase();
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].toLowerCase().equals(sourceTimestampColumnName)) {
                if (columnTypes[i] == Types.TIMESTAMP || columnTypes[i] == Types.DATE) {
                    return i;
                } else {
                    throw new IllegalArgumentException("Source timestamp column '" + sourceTimestampColumnName + "' must be of type TIMESTAMP or DATE but was '" + JDBCType.valueOf(columnTypes[i]).getName() + "'");
                }
            }
        }
        throw new IllegalArgumentException("Source timestamp column '" + sourceTimestampColumnName + "' not found");
    }

    private static void sendLine(LowerCaseCharSequenceHashSet symbols, ResultSet resultSet, int columnCount, String[] columnNames, int[] columnTypes, int timestampIndex, Sender sender, String tableName) throws SQLException {
        sender.table(tableName);
        for (int i = 0; i < columnCount; i++) {
            // symbols first
            String columnName = columnNames[i];
            if (symbols.contains(columnName)) {
                String value = resultSet.getString(i + 1);
                if (value != null) {
                    sender.symbol(columnName, value);
                }
            }
        }

        for (int i = 0; i < columnCount; i++) {
            // symbols first
            String columnName = columnNames[i];
            if (!symbols.contains(columnName)) {
                switch (columnTypes[i]) {
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        long aLong = resultSet.getLong(i + 1);
                        if (!resultSet.wasNull()) {
                            sender.longColumn(columnName, aLong);
                        }
                        break;

                    case Types.DOUBLE:
                    case Types.FLOAT:
                    case Types.REAL:
                        double aDouble = resultSet.getDouble(i + 1);
                        if (!resultSet.wasNull() && !Double.isNaN(aDouble)) {
                            sender.doubleColumn(columnName, aDouble);
                        }
                        break;

                    case Types.BOOLEAN:
                    case Types.BIT:
                        boolean aBoolean = resultSet.getBoolean(i + 1);
                        if (!resultSet.wasNull()) {
                            sender.boolColumn(columnName, aBoolean);
                        }
                        break;

                    case Types.DATE:
                    case Types.TIMESTAMP:
                        if (i != timestampIndex) {
                            long microEpoch = getMicroEpoch(resultSet, i + 1);
                            if (microEpoch != Numbers.LONG_NaN && !resultSet.wasNull()) {
                                sender.timestampColumn(columnName, microEpoch);
                            }
                        }
                        break;

                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                        String value = resultSet.getString(i + 1);
                        if (value != null && !resultSet.wasNull()) {
                            sender.stringColumn(columnName, resultSet.getString(i + 1));
                        }
                        break;

                    default:
                        throw new UnsupportedOperationException("Unsupported column type: " + columnName);
                }
            }
        }

        long microEpoch = getMicroEpoch(resultSet, timestampIndex + 1);
        sender.at(microEpoch * 1000);
    }
}
