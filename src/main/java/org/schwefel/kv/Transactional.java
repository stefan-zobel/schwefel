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

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;

import static org.schwefel.kv.LexicographicByteArrayComparator.lexicographicalCompare;

class Transactional implements Tx {

    private volatile Transaction txn = null;
    private final ReadOptions readOptions;
    private final Stats stats;

    Transactional(Transaction txn, ReadOptions readOptions, Stats stats) {
        this.txn = Objects.requireNonNull(txn);
        this.stats = Objects.requireNonNull(stats).incOpenTxCount();
        this.readOptions = Objects.requireNonNull(readOptions);
    }

    @Override
    public synchronized void commit() {
        validateOwned();
        try {
            txn.commit();
        } catch (RocksDBException e) {
            try {
                rollback();
            } catch (Exception ignore) {
                // ignore
            }
            throw new StoreException(e);
        } finally {
            close();
        }
    }

    @Override
    public synchronized void rollback() {
        if (txn != null) {
            try {
                txn.rollback();
            } catch (RocksDBException e) {
                throw new StoreException(e);
            } finally {
                close();
            }
        }
    }

    @Override
    public synchronized void close() {
        if (txn != null) {
            try {
                txn.close();
            } finally {
                txn = null;
                stats.decOpenTxCount();
            }
        }
    }

    @Override
    public synchronized void disableIndexing() {
        validateOwned();
        txn.disableIndexing();
    }

    @Override
    public synchronized void enableIndexing() {
        validateOwned();
        txn.enableIndexing();
    }

    @Override
    public synchronized void setLockTimeout(long lockTimeoutMillis) {
        validateOwned();
        txn.setLockTimeout(lockTimeoutMillis);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            txn.put(key, value);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized void putIfAbsent(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            if (get(key) == null) {
                txn.put(key, value);
            }
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[] get(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        validateReadOptions();
        try {
            return txn.get(readOptions, key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[][] multiGet(byte[][] keys) {
        Objects.requireNonNull(keys, "keys cannot be null");
        checkInnerKeys(keys);
        validateOwned();
        validateReadOptions();
        try {
            return txn.multiGet(readOptions, keys);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized ForEachKeyValue scanAll() {
        validateOwned();
        validateReadOptions();
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions));
        stats.incOpenCursorsCount();
        it.seekToFirst();
        return new ForEachAll(it, stats, this);
    }

    @Override
    public synchronized ForEachKeyValue scanAll(byte[] beginKey) {
        Objects.requireNonNull(beginKey, "beginKey cannot be null");
        validateOwned();
        validateReadOptions();
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions));
        stats.incOpenCursorsCount();
        it.seek(beginKey);
        return new ForEachAll(it, stats, this);
    }

    @Override
    public synchronized ForEachKeyValue scanRange(byte[] beginKey, byte[] endKey) {
        Objects.requireNonNull(beginKey, "beginKey cannot be null");
        Objects.requireNonNull(endKey, "endKey cannot be null");
        validateOwned();
        validateReadOptions();
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions));
        stats.incOpenCursorsCount();
        it.seek(beginKey);
        return new ForEachRange(it, endKey, stats, this);
    }

    @Override
    public synchronized byte[] findMaxKeyLessThan(byte[] keyPrefix, byte[] upperBound) {
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        Objects.requireNonNull(upperBound, "upperBound cannot be null");
        validateOwned();
        validateReadOptions();
        if (keyPrefix.length >= upperBound.length && lexicographicalCompare(keyPrefix, upperBound) > 0) {
            return null;
        }
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMaxKeyLessThan(it, stats, keyPrefix, upperBound);
    }

    @Override
    public synchronized byte[] findMinKeyGreaterThan(byte[] keyPrefix, byte[] lowerBound) {
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        Objects.requireNonNull(lowerBound, "lowerBound cannot be null");
        validateOwned();
        validateReadOptions();
        if (keyPrefix.length >= lowerBound.length && lexicographicalCompare(keyPrefix, lowerBound) < 0) {
            return null;
        }
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMinKeyGreaterThan(it, stats, keyPrefix, lowerBound);
    }

    @Override
    public byte[] getForUpdate(byte[] key) {
        return getForUpdate(key, true);
    }

    @Override
    public synchronized byte[] getForUpdate(byte[] key, boolean exclusive) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        validateReadOptions();
        try {
            return txn.getForUpdate(readOptions, key, exclusive);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[][] multiGetForUpdate(byte[][] keys) {
        Objects.requireNonNull(keys, "keys cannot be null");
        checkInnerKeys(keys);
        validateOwned();
        validateReadOptions();
        try {
            return txn.multiGetForUpdate(readOptions, keys);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized void undoGetForUpdate(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        txn.undoGetForUpdate(key);
    }

    @Override
    public synchronized void delete(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            txn.delete(key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[] deleteIfPresent(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        byte[] oldVal = null;
        try {
            if ((oldVal = get(key)) != null) {
                txn.delete(key);
            }
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
        return oldVal;
    }

    @Override
    public synchronized void singleDelete(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            txn.singleDelete(key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[] updateIfPresent(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        byte[] oldVal = null;
        try {
            if ((oldVal = get(key)) != null) {
                txn.put(key, value);
            }
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
        return oldVal;
    }

    private void validateReadOptions() {
        if (!readOptions.isOwningHandle()) {
            throw new StoreException("ReadOptions already closed!?");
        }
    }

    private void validateOwned() {
        if (txn == null) {
            throw new StoreException("Tx has already lost ownership");
        }
    }

    private static void checkInnerKeys(byte[][] keys) {
        for (int i = 0; i < keys.length; ++i) {
            if (keys[i] == null) {
                throw new NullPointerException("keys[" + i + "] cannot be null");
            }
        }
    }
}
