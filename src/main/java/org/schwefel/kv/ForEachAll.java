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

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.rocksdb.RocksIterator;

// TODO: not sure if this overcautious early closing approach is a good idea
class ForEachAll extends AbstractForEach {

    private static final Logger logger = Logger.getLogger(ForEachAll.class.getName());

    ForEachAll(RocksIterator iter, Stats stats) {
        super(iter, stats);
    }

    @Override
    public synchronized void forEachRemaining(BiConsumer<byte[], byte[]> action) {
        Objects.requireNonNull(action, "action cannot be null");
        checkOpen();
        try {
            while (iter.isValid()) {
                action.accept(iter.key(), iter.value());
                iter.next();
            }
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "closing ForEachAll", e);
            throw e;
        } finally {
            close();
        }
    }

    @Override
    public synchronized boolean tryAdvance(BiConsumer<byte[], byte[]> action) {
        Objects.requireNonNull(action, "action cannot be null");
        checkOpen();
        if (iter.isValid()) {
            try {
                action.accept(iter.key(), iter.value());
                iter.next();
            } catch (RuntimeException e) {
                close();
                logger.log(Level.WARNING, "closing ForEachAll", e);
                throw e;
            }
            return true;
        }
        close();
        return false;
    }
}
