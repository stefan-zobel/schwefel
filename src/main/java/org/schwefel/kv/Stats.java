/*
 * Copyright 2020 Stefan Zobel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schwefel.kv;

import net.volcanite.util.DoubleStatistics;

public class Stats {

    final DoubleStatistics putTimeNanos = DoubleStatistics.newInstance();
    final DoubleStatistics getTimeNanos = DoubleStatistics.newInstance();
    final DoubleStatistics deleteTimeNanos = DoubleStatistics.newInstance();
    final DoubleStatistics mergeTimeNanos = DoubleStatistics.newInstance();
    final DoubleStatistics batchTimeNanos = DoubleStatistics.newInstance();
    final DoubleStatistics walTimeNanos = DoubleStatistics.newInstance();
    final DoubleStatistics flushTimeNanos = DoubleStatistics.newInstance();
    final DoubleStatistics allOpsTimeNanos = DoubleStatistics.newInstance();

    public Stats() {
        //
    }

    public DoubleStatistics getPutTimeNanos() {
        return putTimeNanos;
    }

    public DoubleStatistics getGetTimeNanos() {
        return getTimeNanos;
    }

    public DoubleStatistics getDeleteTimeNanos() {
        return deleteTimeNanos;
    }

    public DoubleStatistics getMergeTimeNanos() {
        return mergeTimeNanos;
    }

    public DoubleStatistics getBatchTimeNanos() {
        return batchTimeNanos;
    }

    public DoubleStatistics getWalTimeNanos() {
        return walTimeNanos;
    }

    public DoubleStatistics getFlushTimeNanos() {
        return flushTimeNanos;
    }

    public DoubleStatistics getAllOpsTimeNanos() {
        return allOpsTimeNanos;
    }
}
