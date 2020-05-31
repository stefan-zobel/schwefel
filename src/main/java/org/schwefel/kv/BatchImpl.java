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

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

class BatchImpl implements Batch, AutoCloseable {

    private volatile WriteBatch batch;

    BatchImpl() {
        batch = new WriteBatch();
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            batch.put(key, value);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized void delete(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            batch.delete(key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized void deleteRange(byte[] beginKey, byte[] endKey) {
        Objects.requireNonNull(beginKey, "beginKey cannot be null");
        Objects.requireNonNull(endKey, "endKey cannot be null");
        validateOwned();
        try {
            batch.deleteRange(beginKey, endKey);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized void update(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            batch.merge(key, value);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    synchronized WriteBatch cedeOwnership() {
        WriteBatch b = batch;
        batch = null;
        return b;
    }

    public synchronized void close() {
        if (batch != null) {
            try {
                batch.close();
            } finally {
                batch = null;
            }
        }
    }

    private void validateOwned() {
        if (batch == null) {
            throw new StoreException("Batch has already lost ownership");
        }
    }
}
