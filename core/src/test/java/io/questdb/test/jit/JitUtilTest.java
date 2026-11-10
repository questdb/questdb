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


package io.questdb.test.jit;

import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

public class JitUtilTest {
    private static final Log LOG = LogFactory.getLog(JitUtilTest.class);

    @Test
    public void testLibraryWithoutFeatureFunctionHasNoFeatures() {
        // A native library built before FiltersCompiler.getFeatures() existed: the JNI call fails
        // to link, which must read as "no capability", never as an error.
        Assert.assertEquals(0, JitUtil.readFeatures(() -> {
            throw new UnsatisfiedLinkError("'int io.questdb.jit.FiltersCompiler.getFeatures()'");
        }));
        Assert.assertEquals(JitUtil.FEATURE_SYM_IN_SET, JitUtil.readFeatures(() -> JitUtil.FEATURE_SYM_IN_SET));
    }

    @Test
    public void testSymbolInSetOnlyOnX86() {
        final boolean supported = JitUtil.isSymbolInSetSupported();
        LOG.info().$("symbol IN set JIT capability [supported=").$(supported).$(", arch=").$(Os.arch).I$();
        if (Os.arch != Os.ARCH_X86_64) {
            Assert.assertFalse(supported);
        }
        // Stable: probed once.
        Assert.assertEquals(supported, JitUtil.isSymbolInSetSupported());
    }
}
