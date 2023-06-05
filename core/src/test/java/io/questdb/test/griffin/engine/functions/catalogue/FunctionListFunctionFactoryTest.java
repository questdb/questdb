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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.engine.functions.catalogue.FunctionListFunctionFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static io.questdb.griffin.engine.functions.catalogue.FunctionListFunctionFactory.isExcluded;

public class FunctionListFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testFunctions() throws Exception {
        TestUtils.printSql(compiler, sqlExecutionContext, "functions()", sink);
        Assert.assertEquals(expectedFunctions(), extractFunctionsFromSink());
    }

    @Test
    public void testFunctionsWithFilter() throws Exception {
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "SELECT name FROM (SELECT name, count(name) FROM functions() GROUP BY name, type ORDER BY name)",
                sink
        );
        Assert.assertEquals(expectedFunctionNames(), extractFunctionNamesFromSink());
    }

    private static Set<String> expectedFunctionNames() {
        Set<String> names = new HashSet<>();
        for (FunctionFactory factory : ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())) {
            String signature = factory.getSignature();
            String name = signature.substring(0, signature.indexOf('('));
            if (isExcluded(name)) {
                continue;
            }
            names.add(name);
        }
        return names;
    }

    private static Set<String> expectedFunctions() {
        Set<String> lines = new HashSet<>();
        StringSink sink2 = new StringSink();
        for (FunctionFactory factory : ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())) {
            String signature = factory.getSignature();
            String name = signature.substring(0, signature.indexOf('('));
            if (isExcluded(name)) {
                continue;
            }
            sink2.clear();
            sink2.put(name).put('\t');
            sink2.put(signature).put('\t');
            FunctionFactoryDescriptor.translateSignature(name, signature, sink2).put('\t');
            sink2.put(factory.isRuntimeConstant()).put('\t');
            sink2.put(FunctionListFunctionFactory.FunctionFactoryType.getType(factory).name()).put('\n');
            lines.add(sink2.toString());
        }
        return lines;
    }

    private static Set<String> extractFunctionNamesFromSink() {
        return Arrays.stream(sink.toString().split("\n"))
                .skip(1)
                .map(line -> line.split("\t")[0])
                .collect(Collectors.toSet());
    }

    private static Set<String> extractFunctionsFromSink() {
        return Arrays.stream(sink.toString().split("\n"))
                .skip(1)
                .map(line -> line + '\n')
                .collect(Collectors.toSet());
    }
}
