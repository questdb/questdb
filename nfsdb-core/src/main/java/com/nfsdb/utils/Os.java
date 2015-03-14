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

package com.nfsdb.utils;

import com.sun.management.OperatingSystemMXBean;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.management.ManagementFactory;

public final class Os {
    @SuppressFBWarnings({"IICU_INCORRECT_INTERNAL_CLASS_USE", "IICU_INCORRECT_INTERNAL_CLASS_USE"})
    private static final OperatingSystemMXBean bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    private Os() {
    }

    public static long getSystemMemory() {
        return bean.getTotalPhysicalMemorySize();
    }
}
