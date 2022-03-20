/*
 * Copyright 2021, 2022 Stefan Zobel
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
package org.schwefel.kv.kueue;

import java.util.concurrent.TimeUnit;

/**
 * A simple RocksDB-based in-process durable queue.
 */
public interface Kueue {

    void put(byte[] value);
    byte[] take() throws InterruptedException;
    byte[] take(long timeout, TimeUnit unit) throws InterruptedException;
    boolean accept(KueueMsgConsumer consumer);
    boolean accept(long timeout, TimeUnit unit, KueueMsgConsumer consumer) throws InterruptedException;
    long size();
    boolean isEmpty();
    boolean isClosed();
    String identifier();
    void clear();
    long totalPuts();
    long totalTakes();
    KueueManager getKueueManager();
}
