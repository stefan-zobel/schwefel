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

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.TransactionOptions;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.schwefel.kv.LexicographicByteArrayComparator.lexicographicalCompare;

public final class KVStore implements StoreOps, KindManagement {

    private static final long FLUSH_TIME_WINDOW_MILLIS = 985L;
    private static final long FLUSH_BATCH_SIZE = 20_000L;
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final Logger logger = Logger.getLogger(KVStore.class.getName());

    private volatile boolean open = false;
    private long totalSinceLastFsync = 0L;
    private long lastSync;

    private TransactionDB txnDb;
    private TransactionDBOptions txnDbOptions;
    private TransactionOptions txnOpts;
    private DBOptions options;
    private ColumnFamilyOptions columnFamilyOptions;
    private WriteOptions writeOptions;
    private ReadOptions readOptions;
    private FlushOptions flushOptions;
    private FlushOptions flushOptionsNoWait;
    private final HashMap<String, KindImpl> kinds = new HashMap<>(); 
    private final String path;
    private final Stats stats = new Stats();

    public KVStore(Path dir) {
        this.path = (String) wrapEx(() -> Objects.requireNonNull(dir).toFile().getCanonicalPath());
        wrapEx(() -> Files.createDirectories(dir));
        open();
    }

    private void open() {
        options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setErrorIfExists(false);
        options.setKeepLogFileNum(2);
        options.setDeleteObsoleteFilesPeriodMicros(3600000000L);
        options.setIncreaseParallelism(Math.max(Runtime.getRuntime().availableProcessors(), 2));
        columnFamilyOptions = new ColumnFamilyOptions();
        writeOptions = new WriteOptions();
        readOptions = new ReadOptions();
        flushOptions = new FlushOptions();
        flushOptions.setWaitForFlush(true);
        flushOptionsNoWait = new FlushOptions();
        flushOptionsNoWait.setWaitForFlush(false);
        txnDbOptions = new TransactionDBOptions();
        txnDb = (TransactionDB) wrapEx(() -> openDatabase());
        txnOpts = new TransactionOptions();
        open = true;
        lastSync = System.currentTimeMillis();
    }

    private TransactionDB openDatabase() throws RocksDBException {
        try (Options opts = new Options(options, columnFamilyOptions)) {
            List<byte[]> families = RocksDB.listColumnFamilies(opts, path);
            ArrayList<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
            for (byte[] cfName : families) {
                cfDescs.add(new ColumnFamilyDescriptor(cfName, columnFamilyOptions));
            }
            if (cfDescs.isEmpty()) {
                cfDescs.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
                        columnFamilyOptions));
            }
            ArrayList<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescs.size());
            TransactionDB txnDb = TransactionDB.open(options, txnDbOptions, path, cfDescs, cfHandles);
            for (ColumnFamilyHandle handle : cfHandles) {
                KindImpl kind = new KindImpl(handle.getName(), handle);
                kinds.put(kind.name(), kind);
            }
            return txnDb;
        }
    }

    @Override
    public synchronized void close() {
        if (!isOpen()) {
            return;
        }
        open = false;
        ignoreEx(() -> syncWAL());
        ignoreEx(() -> flush());
        closeCfHandles();
        kinds.clear();
        close(txnDb);
        close(txnDbOptions);
        close(txnOpts);
        close(columnFamilyOptions);
        close(writeOptions);
        close(readOptions);
        close(flushOptions);
        close(flushOptionsNoWait);
        close(options);
        txnDb = null;
        txnDbOptions = null;
        txnOpts = null;
        columnFamilyOptions = null;
        writeOptions = null;
        readOptions = null;
        flushOptions = null;
        flushOptionsNoWait = null;
        options = null;
    }

    private void closeCfHandles() {
        for (KindImpl kind : kinds.values()) {
            close(kind.handle());
        }
    }

    @Override
    public KindManagement getKindManagement() {
        validateOpen();
        return this;
    }

    @Override
    public synchronized Set<Kind> getKinds() {
        validateOpen();
        return new TreeSet<>(kinds.values());
    }

    @Override
    public synchronized Kind getKind(String kindName) {
        validateOpen();
        return kinds.get(kindName);
    }

    @Override
    public synchronized Kind getOrCreateKind(String kindName) {
        if (Objects.requireNonNull(kindName).isEmpty()) {
            throw new IllegalArgumentException("kindName: ");
        }
        validateOpen();
        Kind kind = getKind(kindName);
        if (kind == null) {
            kind = (Kind) wrapEx(() -> createKind(kindName));
        }
        return kind;
    }

    private Kind createKind(String kindName) throws RocksDBException {
        ColumnFamilyHandle handle = txnDb
                .createColumnFamily(new ColumnFamilyDescriptor(kindName.getBytes(UTF8), columnFamilyOptions));
        KindImpl kind = new KindImpl(handle.getName(), handle);
        kinds.put(kind.name(), kind);
        return kind;
    }

    @Override
    public synchronized Kind getDefaultKind() {
        validateOpen();
        return getKind("default");
    }

    @Override
    public synchronized void put(Kind kind, byte[] key, byte[] value) {
        long start = System.nanoTime();
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOpen();
        try {
            put_(kind, key, value);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        } finally {
            stats.allOpsTimeNanos.accept(System.nanoTime() - start);
        }
    }

    private void put_(Kind kind, byte[] key, byte[] value) throws RocksDBException {
        long putStart = System.nanoTime();
        try (Transaction txn = txnDb.beginTransaction(writeOptions, txnOpts)) {
            txn.put(((KindImpl) kind).handle(), key, value);
            txn.commit();
            stats.putTimeNanos.accept(System.nanoTime() - putStart);
            occasionalWalSync();
        }
    }

    private void occasionalWalSync() {
        ++totalSinceLastFsync;
        if (System.currentTimeMillis() - lastSync >= FLUSH_TIME_WINDOW_MILLIS) {
            syncWAL();
            lastSync = System.currentTimeMillis();
            totalSinceLastFsync = 0L;
        } else if (totalSinceLastFsync % FLUSH_BATCH_SIZE == 0L) {
            syncWAL();
            lastSync = System.currentTimeMillis();
            totalSinceLastFsync = 0L;
        }
    }

    @Override
    public synchronized void putIfAbsent(Kind kind, byte[] key, byte[] value) {
        long start = System.nanoTime();
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOpen();
        try {
            if (get_(kind, key) == null) {
                put_(kind, key, value);
            }
        } catch (RocksDBException e) {
            throw new StoreException(e);
        } finally {
            stats.allOpsTimeNanos.accept(System.nanoTime() - start);
        }
    }

    @Override
    public synchronized byte[] deleteIfPresent(Kind kind, byte[] key) {
        long start = System.nanoTime();
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOpen();
        byte[] oldVal = null;
        try {
            if ((oldVal = get_(kind, key)) != null) {
                delete_(kind, key);
            }
        } catch (RocksDBException e) {
            throw new StoreException(e);
        } finally {
            stats.allOpsTimeNanos.accept(System.nanoTime() - start);
        }
        return oldVal;
    }

    @Override
    public synchronized byte[] updateIfPresent(Kind kind, byte[] key, byte[] value) {
        long start = System.nanoTime();
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOpen();
        byte[] oldVal = null;
        try {
            if ((oldVal = get_(kind, key)) != null) {
                put_(kind, key, value);
            }
        } catch (RocksDBException e) {
            throw new StoreException(e);
        } finally {
            stats.allOpsTimeNanos.accept(System.nanoTime() - start);
        }
        return oldVal;
    }

    @Override
    public synchronized byte[] get(Kind kind, byte[] key) {
        long start = System.nanoTime();
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOpen();
        try {
            return get_(kind, key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        } finally {
            stats.allOpsTimeNanos.accept(System.nanoTime() - start);
        }
    }

    private byte[] get_(Kind kind, byte[] key) throws RocksDBException {
        long start = System.nanoTime();
        try {
            return txnDb.get(((KindImpl) kind).handle(), readOptions, key);
        } finally {
            stats.getTimeNanos.accept(System.nanoTime() - start);
        }
    }

    @Override
    public synchronized void delete(Kind kind, byte[] key) {
        long start = System.nanoTime();
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(key, "key cannot be null");
        validateOpen();
        try {
            delete_(kind, key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        } finally {
            stats.allOpsTimeNanos.accept(System.nanoTime() - start);
        }
    }

    private void delete_(Kind kind, byte[] key) throws RocksDBException {
        long delStart = System.nanoTime();
        try (Transaction txn = txnDb.beginTransaction(writeOptions, txnOpts)) {
            txn.delete(((KindImpl) kind).handle(), key);
            txn.commit();
            stats.deleteTimeNanos.accept(System.nanoTime() - delStart);
            occasionalWalSync();
        }
    }

    @Override
    public synchronized void writeBatch(Batch batch) {
        long start = System.nanoTime();
        Objects.requireNonNull(batch, "batch cannot be null");
        validateOpen();
        WriteBatch wb = ((BatchImpl) batch).cedeOwnership();
        if (wb != null) {
            try {
                txnDb.write(writeOptions, wb);
            } catch (RocksDBException e) {
                throw new StoreException(e);
            } finally {
                close(wb);
                long delta = System.nanoTime() - start;
                stats.allOpsTimeNanos.accept(delta);
                stats.batchTimeNanos.accept(delta);
                occasionalWalSync();
            }
        }
    }

    @Override
    public synchronized void syncWAL() {
        if (isOpen()) {
            long start = System.nanoTime();
            try {
                if (txnDb.isOwningHandle()) {
                    txnDb.flushWal(true);
                }
            } catch (RocksDBException e) {
                throw new StoreException(e);
            } finally {
                long delta = System.nanoTime() - start;
                stats.allOpsTimeNanos.accept(delta);
                stats.walTimeNanos.accept(delta);
            }
        }
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    private void validateOpen() {
        if (!isOpen()) {
            throw new StoreException("KVStore " + path + " is closed");
        }
    }

    @Override
    public synchronized void flush() {
        flush_(flushOptions);
    }

    @Override
    public synchronized void flushNoWait() {
        flush_(flushOptionsNoWait);
    }

    private void flush_(FlushOptions flushOptions) {
        if (isOpen()) {
            long start = System.nanoTime();
            try {
                txnDb.flush(flushOptions);
            } catch (RocksDBException e) {
                throw new StoreException(e);
            } finally {
                long delta = System.nanoTime() - start;
                stats.allOpsTimeNanos.accept(delta);
                stats.flushTimeNanos.accept(delta);
            }
        }
    }

    @Override
    public synchronized ForEachKeyValue scanAll(Kind kind) {
        Objects.requireNonNull(kind, "kind cannot be null");
        validateOpen();
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        it.seekToFirst();
        return new ForEachAll(it, stats, this);
    }

    @Override
    public synchronized ForEachKeyValue scanAll(Kind kind, byte[] beginKey) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(beginKey, "beginKey cannot be null");
        validateOpen();
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        it.seek(beginKey);
        return new ForEachAll(it, stats, this);
    }

    @Override
    public synchronized ForEachKeyValue scanRange(Kind kind, byte[] beginKey, byte[] endKey) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(beginKey, "beginKey cannot be null");
        Objects.requireNonNull(endKey, "endKey cannot be null");
        validateOpen();
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        it.seek(beginKey);
        return new ForEachRange(it, endKey, stats, this);
    }

    @Override
    public synchronized byte[] findMinKey(Kind kind) {
        Objects.requireNonNull(kind, "kind cannot be null");
        validateOpen();
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMinKey(it, stats);
    }

    @Override
    public synchronized byte[] findMinKeyByPrefix(Kind kind, byte[] keyPrefix) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        validateOpen();
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMinKey(it, stats, keyPrefix);
    }

    @Override
    public synchronized byte[] findMaxKey(Kind kind) {
        Objects.requireNonNull(kind, "kind cannot be null");
        validateOpen();
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMaxKey(it, stats);
    }

    @Override
    public synchronized byte[] findMaxKeyByPrefix(Kind kind, byte[] keyPrefix) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        validateOpen();
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMaxKey(it, stats, keyPrefix);
    }

    @Override
    public synchronized byte[] findMinKeyByLowerBound(Kind kind, byte[] lowerBound) { // XXX
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(lowerBound, "lowerBound cannot be null");
        validateOpen();
        Slice slice = new Slice(lowerBound);
        ReadOptions opt = new ReadOptions();
        opt.setIterateLowerBound(slice);
        opt.setIterateUpperBound(null);
        // TODO Auto-generated method stub
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle(), opt));
        stats.incOpenCursorsCount();
        byte[] key = null;
        if (it.isOwningHandle()) {
            it.seekToFirst();
            if (it.isValid()) {
                key = it.key();
            }
        }
        if (it.isOwningHandle()) {
            it.close();
            stats.decOpenCursorsCount();
        }
        // TODO Auto-generated method stub
        opt.close();
        slice.close();
        return key;
    }

    @Override
    public synchronized byte[] findMaxKeyLessThan(Kind kind, byte[] keyPrefix, byte[] upperBound) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        Objects.requireNonNull(upperBound, "upperBound cannot be null");
        validateOpen();
        if (keyPrefix.length >= upperBound.length && lexicographicalCompare(keyPrefix, upperBound) > 0) {
            return null;
        }
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMaxKeyLessThan(it, stats, keyPrefix, upperBound);
    }

    @Override
    public synchronized byte[] findMinKeyGreaterThan(Kind kind, byte[] keyPrefix, byte[] lowerBound) {
        Objects.requireNonNull(kind, "kind cannot be null");
        Objects.requireNonNull(keyPrefix, "keyPrefix cannot be null");
        Objects.requireNonNull(lowerBound, "lowerBound cannot be null");
        validateOpen();
        if (keyPrefix.length >= lowerBound.length && lexicographicalCompare(keyPrefix, lowerBound) < 0) {
            return null;
        }
        RocksIterator it = Objects.requireNonNull(txnDb.newIterator(((KindImpl) kind).handle()));
        stats.incOpenCursorsCount();
        return MinMaxKeyIt.findMinKeyGreaterThan(it, stats, keyPrefix, lowerBound);
    }

    @Override
    public Batch createBatch() {
        return new BatchImpl();
    }

    @Override
    public synchronized Tx startTx() {
        return new Transactional(txnDb.beginTransaction(writeOptions, txnOpts), readOptions, getStats());
    }

    @Override
    public synchronized Stats getStats() {
        return stats;
    }

    private static void close(AutoCloseable ac) {
        if (ac != null) {
            try {
                ac.close();
            } catch (Exception ignore) {
                logger.log(Level.INFO, "", ignore);
            }
        }
    }

    private static interface ThrowingSupplier {
        Object get() throws Exception;
    }

    private static Object wrapEx(ThrowingSupplier block) {
        try {
            return block.get();
        } catch (Exception e) {
            logger.log(Level.WARNING, "", e);
            throw new StoreException(e);
        }
    }

    private static void ignoreEx(Runnable block) {
        try {
            block.run();
        } catch (Exception ignore) {
            logger.log(Level.INFO, "", ignore);
        }
    }
}
