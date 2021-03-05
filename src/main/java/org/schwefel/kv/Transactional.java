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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.rocksdb.ColumnFamilyHandle;
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
    public synchronized void put(Kind kind, byte[] key, byte[] value) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            txn.put(((KindImpl) kind).handle(), key, value);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized void putIfAbsent(Kind kind, byte[] key, byte[] value) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            if (get(kind, key) == null) {
                txn.put(((KindImpl) kind).handle(), key, value);
            }
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[] get(Kind kind, byte[] key) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        validateReadOptions();
        try {
            return txn.get(((KindImpl) kind).handle(), readOptions, key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[][] multiGet(List<Kind> kinds, byte[][] keys) {
        Objects.requireNonNull(kinds, "kinds cannot be null");
        Objects.requireNonNull(keys, "keys cannot be null");
        if (kinds.size() != keys.length) {
            throw new IllegalArgumentException(
                    "Each key must have an associated Kind. kinds = " + kinds.size() + " != keys = " + keys.length);
        }
        checkInnerKeys(keys);
        validateOwned();
        validateReadOptions();
        try {
            return txn.multiGet(readOptions, toCfHandleList(kinds), keys);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized ForEachKeyValue scanAll(Kind kind) {
        Objects.requireNonNull(kind, "kind cannot be null");
        validateOwned();
        validateReadOptions();
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions, ((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        it.seekToFirst();
        return new ForEachAll(it, stats, this);
    }

    @Override
    public synchronized ForEachKeyValue scanAll(Kind kind, byte[] beginKey) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(beginKey, "beginKey cannot be null");
        validateOwned();
        validateReadOptions();
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions, ((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        it.seek(beginKey);
        return new ForEachAll(it, stats, this);
    }

    @Override
    public synchronized ForEachKeyValue scanRange(Kind kind, byte[] beginKey, byte[] endKey) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(beginKey, "beginKey cannot be null");
        Objects.requireNonNull(endKey, "endKey cannot be null");
        validateOwned();
        validateReadOptions();
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions, ((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        it.seek(beginKey);
        return new ForEachRange(it, endKey, stats, this);
    }

    @Override
    public synchronized byte[] findMinKey(Kind kind, byte[] keyPrefix) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        validateOwned();
        validateReadOptions();
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions, ((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMinKey(it, stats, keyPrefix);
    }

    @Override
    public synchronized byte[] findMaxKey(Kind kind, byte[] keyPrefix) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        validateOwned();
        validateReadOptions();
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions, ((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMaxKey(it, stats, keyPrefix);
    }

    @Override
    public synchronized byte[] findMaxKeyLessThan(Kind kind, byte[] keyPrefix, byte[] upperBound) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        Objects.requireNonNull(upperBound, "upperBound cannot be null");
        validateOwned();
        validateReadOptions();
        if (keyPrefix.length >= upperBound.length && lexicographicalCompare(keyPrefix, upperBound) > 0) {
            return null;
        }
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions, ((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMaxKeyLessThan(it, stats, keyPrefix, upperBound);
    }

    @Override
    public synchronized byte[] findMinKeyGreaterThan(Kind kind, byte[] keyPrefix, byte[] lowerBound) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        Objects.requireNonNull(lowerBound, "lowerBound cannot be null");
        validateOwned();
        validateReadOptions();
        if (keyPrefix.length >= lowerBound.length && lexicographicalCompare(keyPrefix, lowerBound) < 0) {
            return null;
        }
        RocksIterator it = Objects.requireNonNull(txn.getIterator(readOptions, ((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMinKeyGreaterThan(it, stats, keyPrefix, lowerBound);
    }

    @Override
    public byte[] getForUpdate(Kind kind, byte[] key) {
        return getForUpdate(kind, key, true);
    }

    @Override
    public synchronized byte[] getForUpdate(Kind kind, byte[] key, boolean exclusive) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        validateReadOptions();
        try {
            return txn.getForUpdate(readOptions, ((KindImpl) kind).handle(), key, exclusive);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[][] multiGetForUpdate(List<Kind> kinds, byte[][] keys) {
        Objects.requireNonNull(kinds, "kinds cannot be null");
        Objects.requireNonNull(keys, "keys cannot be null");
        if (kinds.size() != keys.length) {
            throw new IllegalArgumentException(
                    "Each key must have an associated Kind. kinds = " + kinds.size() + " != keys = " + keys.length);
        }
        checkInnerKeys(keys);
        validateOwned();
        validateReadOptions();
        try {
            return txn.multiGetForUpdate(readOptions, toCfHandleList(kinds), keys);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    private static List<ColumnFamilyHandle> toCfHandleList(List<Kind> kinds) {
        return kinds.stream().map(k -> ((KindImpl) k).handle()).collect(Collectors.toList());
    }

    @Override
    public synchronized void undoGetForUpdate(Kind kind, byte[] key) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        txn.undoGetForUpdate(((KindImpl) kind).handle(), key);
    }

    @Override
    public synchronized void delete(Kind kind, byte[] key) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            txn.delete(((KindImpl) kind).handle(), key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[] deleteIfPresent(Kind kind, byte[] key) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        byte[] oldVal = null;
        try {
            if ((oldVal = get(kind, key)) != null) {
                txn.delete(((KindImpl) kind).handle(), key);
            }
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
        return oldVal;
    }

    @Override
    public synchronized void singleDelete(Kind kind, byte[] key) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        try {
            txn.singleDelete(((KindImpl) kind).handle(), key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public synchronized byte[] updateIfPresent(Kind kind, byte[] key, byte[] value) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        byte[] oldVal = null;
        try {
            if ((oldVal = get(kind, key)) != null) {
                txn.put(((KindImpl) kind).handle(), key, value);
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
