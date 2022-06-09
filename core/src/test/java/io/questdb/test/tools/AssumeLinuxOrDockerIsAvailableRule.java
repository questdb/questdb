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

package io.questdb.test.tools;

import io.questdb.std.Os;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.DockerClientFactory;

import static org.junit.Assume.assumeTrue;

/**
 * This is useful for tests which uses Docker: We want them to always run on Linux.
 * Docker on Linux is easy and it should always be available. If Docker is unavailable
 * then we want a test to fail.
 *
 * Other platforms - such as MacOs or Windows - have more complicated relationship with Docker
 * and we cannot assume Docker to always available. This is even more complicated on build server.
 * Hence, we check Docker availability and when it's not there then the rule will skip a test.
 *
 */
public final class AssumeLinuxOrDockerIsAvailableRule implements TestRule {
    public static final TestRule INSTANCE = new AssumeLinuxOrDockerIsAvailableRule();

    private AssumeLinuxOrDockerIsAvailableRule() {

    }
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                boolean isLinux = (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64);
                assumeTrue(isLinux || isDockerAvailable());
                base.evaluate();
            }
        };
    }

    private static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }
}
