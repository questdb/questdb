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

package io.questdb.log;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.TreeSet;

public class LogAlertManagerWriterTest {
    @Test
    public void testProps() {
        Properties expected = new Properties();
        expected.put("Status", "firing");
        expected.put("Labels.alertname", "QuestDbInstanceLogs");
        expected.put("Labels.service", "questdb");
        expected.put("Labels.category", "application-logs");
        expected.put("Labels.severity", "critical");
        expected.put("Labels.namespace", "$NAMESPACE");
        expected.put("Labels.cluster", "$CLUSTER_NAME");
        expected.put("Labels.instance", "$INSTANCE_NAME");
        expected.put("Labels.orgid", "$ORGID");
        expected.put("Annotations.description", "ERROR/${ORGID}/${NAMESPACE}/${CLUSTER_NAME}/$INSTANCE_NAME");
        expected.put("Annotations.message", "");

        Properties alertProperties = new Properties(expected);
        alertProperties.put("Status", "UNEXPECTED");
        alertProperties.put("Labels.alertname", "UNEXPECTED");
        alertProperties.put("Labels.service", "UNEXPECTED");
        alertProperties.put("Labels.category", "UNEXPECTED");
        alertProperties.put("Labels.severity", "UNEXPECTED");
        alertProperties.put("Labels.namespace", "UNEXPECTED");
        alertProperties.put("Labels.cluster", "UNEXPECTED");
        alertProperties.put("Labels.instance", "UNEXPECTED");
        alertProperties.put("Labels.orgid", "UNEXPECTED");
        alertProperties.put("Annotations.description", "UNEXPECTED");
        alertProperties.put("Annotations.message", "UNEXPECTED");

        String location = LogAlertManagerWriter.DEFAULT_ALERT_TPT_FILE;
        try (InputStream is = LogFactory.class.getResourceAsStream(location)) {
            if (is == null) {
                throw new LogError("Cannot read " + location);
            }
            alertProperties.load(is);
        } catch (IOException e) {
            throw new LogError("Cannot read " + location, e);
        }
        Assert.assertEquals(expected, alertProperties);
    }
}
