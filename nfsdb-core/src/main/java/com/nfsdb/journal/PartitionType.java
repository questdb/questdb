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

package com.nfsdb.journal;

/**
 * Setting partition type on JournalKey to override default settings in nfsdb.xml.
 */
public enum PartitionType {
    DAY, MONTH, YEAR,
    /**
     * Data is not partitioned at all,
     * all data is stored in a single directory
     */
    NONE,
    /**
     * Setting partition type to DEFAULT will use whatever partition type is specified in nfsdb.xml.
     */
    DEFAULT
}
