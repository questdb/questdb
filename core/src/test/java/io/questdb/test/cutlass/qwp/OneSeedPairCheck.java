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

package io.questdb.test.cutlass.qwp;

import io.questdb.log.Log;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Seeds the one Rnd a test draws every random choice from, before the first {@code @Before}
 * method of the test, and keeps the test to it: the pair of seeds the Rnd logs then replays the
 * test, while {@code -Dfuzz.s0} and {@code -Dfuzz.s1} seed every Rnd alike and cannot give back
 * the seeds of a second one. As a rule, it fails a test whose code seeds a Rnd of its own through
 * {@link TestUtils#generateRandom(Log)} from before its first {@code @Before} method to after its
 * last {@code @After} method; as {@link #CLASS_RULE}, a test class whose code seeds one outside its
 * tests, such as in a {@code @BeforeClass} method. A test that fails as well reports both failures,
 * since the seeds it logged cannot replay it, and a skipped one fails all the same. The code of a
 * test is the code of its class, of its superclasses and of the classes nested in them; the Rnds
 * that other classes seed for their own use, such as QueryAssertion for its checks, do not count.
 */
public class OneSeedPairCheck implements TestRule {
    public static final TestRule CLASS_RULE = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            final Class<?> testClass = description.getTestClass();
            final long seededCount = getSeededCount(testClass);
            testSeededCount.set(0);
            final List<Throwable> errors = evaluateCollecting(base);
            final long outsideCount = getSeededCount(testClass) - seededCount - testSeededCount.get();
            throwErrors(errors, outsideCount, " outside its tests");
        }
    };
    private static final String REPLAY_NOTE = "; -Dfuzz.s0 and -Dfuzz.s1 seed every Rnd alike, so they can replay"
            + " a test only if it draws from random alone";
    // the Rnds that the code of the tests of the running class seeded of its own while they ran
    private static final AtomicLong testSeededCount = new AtomicLong();
    private final Log log;
    private Rnd random;

    public OneSeedPairCheck(Log log) {
        this.log = log;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                final Class<?> testClass = description.getTestClass();
                final long seededCount = getSeededCount(testClass);
                random = TestUtils.generateRandom(log);
                final List<Throwable> errors = evaluateCollecting(base);
                final long ownCount = getSeededCount(testClass) - seededCount;
                testSeededCount.addAndGet(ownCount);
                throwErrors(errors, ownCount, "");
            }
        };
    }

    /**
     * Returns the Rnd of the running test.
     */
    public Rnd getRandom() {
        return random;
    }

    private static List<Throwable> evaluateCollecting(Statement statement) {
        final List<Throwable> errors = new ArrayList<>();
        try {
            statement.evaluate();
        } catch (Throwable th) {
            errors.add(th);
        }
        return errors;
    }

    // the Rnds TestUtils.generateRandom(Log) has seeded for the code of testClass
    private static long getSeededCount(Class<?> testClass) {
        final Set<Class<?>> nestHosts = new HashSet<>();
        long count = 0;
        for (Class<?> cls = testClass; cls != null; cls = cls.getSuperclass()) {
            if (nestHosts.add(cls.getNestHost())) {
                count += TestUtils.getGeneratedRandomCount(cls);
            }
        }
        return count;
    }

    private static void throwErrors(List<Throwable> errors, long ownCount, String where) throws Exception {
        if (ownCount != 0) {
            errors.removeIf(error -> error instanceof AssumptionViolatedException);
            errors.add(new AssertionError("seeded " + ownCount + " Rnd(s) of its own through"
                    + " TestUtils.generateRandom(Log)" + where + REPLAY_NOTE));
        }
        MultipleFailureException.assertEmpty(errors);
    }
}
