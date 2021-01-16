/*
 * Copyright 2020, 2021 Stefan Zobel
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

import java.util.Objects;
import java.util.function.BiConsumer;

import org.rocksdb.RocksIterator;

abstract class AbstractForEach implements ForEachKeyValue {

    volatile RocksIterator iter;
    private volatile BasicOps ops;
    private final Stats stats;

    AbstractForEach(RocksIterator iter, Stats stats, BasicOps ops) {
        this.iter = Objects.requireNonNull(iter);
        this.ops = ops;
        this.stats = stats;
    }

    @Override
    public void close() {
        if (isOpen()) {
            try {
                if (iter.isOwningHandle()) {
                    iter.close();
                    stats.decOpenCursorsCount();
                }
            } finally {
                iter = null;
                ops = null;
            }
        }
    }

    @Override
    public abstract void forEachRemaining(BiConsumer<byte[], byte[]> action);

    @Override
    public abstract boolean tryAdvance(BiConsumer<byte[], byte[]> action);

    @Override
    public BasicOps ops() {
        return ops;
    }

    private boolean isOpen() {
        return iter != null;
    }

    void checkOpen() {
        if (!isOpen()) {
            throw new StoreException("ForEachKeyValue is exhausted or closed");
        }
    }
}
