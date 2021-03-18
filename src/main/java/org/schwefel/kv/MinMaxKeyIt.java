/*
 * Copyright 2021 Stefan Zobel
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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;

import static org.schwefel.kv.LexicographicByteArrayComparator.lexicographicalCompare;

import java.util.Objects;

/*
 * TODO: use the ByteBuffer Iterator methods instead of the byte[] array methods
 */
final class MinMaxKeyIt {

    static byte[] findMaxKeyLessThan(RocksIterator iter, Stats stats, byte[] keyPrefix, byte[] upperBound) {
        try {
            if (iter.isOwningHandle()) {
                iter.seekForPrev(upperBound);
                byte[] key = null;
                if (iter.isValid() && (key = iter.key()) != null && lexicographicalCompare(key, upperBound) == 0) {
                    iter.prev();
                }
                while (iter.isValid() && (key = iter.key()) != null
                        && prefixOfKeyOtherThanKeyPrefix(key, keyPrefix, GREATER)) {
                    iter.prev();
                }
                if (keyStartsWithPrefix(key, keyPrefix) && lexicographicalCompare(key, upperBound) < 0) {
                    return key;
                }
            }
            return null;
        } finally {
            if (iter.isOwningHandle()) {
                iter.close();
                stats.decOpenCursorsCount();
            }
        }
    }

    static byte[] findMinKeyGreaterThan(RocksIterator iter, Stats stats, byte[] keyPrefix, byte[] lowerBound) {
        try {
            if (iter.isOwningHandle()) {
                iter.seek(lowerBound);
                byte[] key = null;
                if (iter.isValid() && (key = iter.key()) != null && lexicographicalCompare(key, lowerBound) == 0) {
                    iter.next();
                }
                while (iter.isValid() && (key = iter.key()) != null
                        && prefixOfKeyOtherThanKeyPrefix(key, keyPrefix, LESS)) {
                    iter.next();
                }
                if (keyStartsWithPrefix(key, keyPrefix) && lexicographicalCompare(key, lowerBound) > 0) {
                    return key;
                }
            }
            return null;
        } finally {
            if (iter.isOwningHandle()) {
                iter.close();
                stats.decOpenCursorsCount();
            }
        }
    }

    static byte[] findMinKey(RocksIterator iter, Stats stats) {
        try {
            if (iter.isOwningHandle()) {
                iter.seekToFirst();
                if (iter.isValid()) {
                    return iter.key();
                }
            }
            return null;
        } finally {
            if (iter.isOwningHandle()) {
                iter.close();
                stats.decOpenCursorsCount();
            }
        }
    }

    static byte[] findMinKey(RocksIterator iter, Stats stats, byte[] keyPrefix) {
        try {
            if (iter.isOwningHandle()) {
                iter.seek(keyPrefix);
                if (iter.isValid()) {
                    byte[] key = iter.key();
                    if (keyStartsWithPrefix(key, keyPrefix)) {
                        return key;
                    }
                }
            }
            return null;
        } finally {
            if (iter.isOwningHandle()) {
                iter.close();
                stats.decOpenCursorsCount();
            }
        }
    }

    static byte[] findMaxKey(RocksIterator iter, Stats stats) {
        try {
            if (iter.isOwningHandle()) {
                iter.seekToLast();
                if (iter.isValid()) {
                    return iter.key();
                }
            }
            return null;
        } finally {
            if (iter.isOwningHandle()) {
                iter.close();
                stats.decOpenCursorsCount();
            }
        }
    }

    static byte[] findMaxKey(RocksIterator iter, Stats stats, byte[] keyPrefix) {
        try {
            if (iter.isOwningHandle()) {
                iter.seek(keyPrefix);
                byte[] key = null;
                byte[] max = null;
                while (iter.isValid() && (key = iter.key()) != null && keyStartsWithPrefix(key, keyPrefix)) {
                    max = key;
                    iter.next();
                }
                if (keyStartsWithPrefix(max, keyPrefix)) {
                    return max;
                }
            }
            return null;
        } finally {
            if (iter.isOwningHandle()) {
                iter.close();
                stats.decOpenCursorsCount();
            }
        }
    }

    static byte[] findMinKeyByLowerBound(TransactionDB txnDb, ColumnFamilyHandle cfHandle, Stats stats, byte[] lowerBound) {
        //@formatter:off
        try (Slice slice = new Slice(lowerBound);
             ReadOptions opt = new ReadOptions()) {
            opt.setIterateLowerBound(slice);
            opt.setIterateUpperBound(null);
            RocksIterator iter = Objects.requireNonNull(txnDb.newIterator(cfHandle, opt));
            try {
                stats.incOpenCursorsCount();
                if (iter.isOwningHandle()) {
                    iter.seekToFirst();
                    if (iter.isValid()) {
                        return iter.key();
                    }
                }
                return null;
            } finally {
                if (iter.isOwningHandle()) {
                    iter.close();
                    stats.decOpenCursorsCount();
                }
            }
        }
        //@formatter:on
    }

    static byte[] findMaxKeyByUpperBound(TransactionDB txnDb, ColumnFamilyHandle cfHandle, Stats stats, byte[] upperBound) {
        //@formatter:off
        try (Slice slice = new Slice(upperBound);
             ReadOptions opt = new ReadOptions()) {
            opt.setIterateLowerBound(null);
            opt.setIterateUpperBound(slice);
            RocksIterator iter = Objects.requireNonNull(txnDb.newIterator(cfHandle, opt));
            try {
                stats.incOpenCursorsCount();
                if (iter.isOwningHandle()) {
                    iter.seekToLast();
                    if (iter.isValid()) {
                        return iter.key();
                    }
                }
                return null;
            } finally {
                if (iter.isOwningHandle()) {
                    iter.close();
                    stats.decOpenCursorsCount();
                }
            }
        }
        //@formatter:on
    }

    static byte[] findMinKeyByLowerBound(Transaction txn, ColumnFamilyHandle cfHandle, Stats stats, byte[] lowerBound) {
        //@formatter:off
        try (Slice slice = new Slice(lowerBound);
             ReadOptions opt = new ReadOptions()) {
            opt.setIterateLowerBound(slice);
            opt.setIterateUpperBound(null);
            RocksIterator iter = Objects.requireNonNull(txn.getIterator(opt, cfHandle));
            try {
                stats.incOpenCursorsCount();
                if (iter.isOwningHandle()) {
                    iter.seekToFirst();
                    if (iter.isValid()) {
                        return iter.key();
                    }
                }
                return null;
            } finally {
                if (iter.isOwningHandle()) {
                    iter.close();
                    stats.decOpenCursorsCount();
                }
            }
        }
        //@formatter:on
    }

    static byte[] findMaxKeyByUpperBound(Transaction txn, ColumnFamilyHandle cfHandle, Stats stats, byte[] upperBound) {
        //@formatter:off
        try (Slice slice = new Slice(upperBound);
             ReadOptions opt = new ReadOptions()) {
            opt.setIterateLowerBound(null);
            opt.setIterateUpperBound(slice);
            RocksIterator iter = Objects.requireNonNull(txn.getIterator(opt, cfHandle));
            try {
                stats.incOpenCursorsCount();
                if (iter.isOwningHandle()) {
                    iter.seekToLast();
                    if (iter.isValid()) {
                        return iter.key();
                    }
                }
                return null;
            } finally {
                if (iter.isOwningHandle()) {
                    iter.close();
                    stats.decOpenCursorsCount();
                }
            }
        }
        //@formatter:on
    }

    private static boolean prefixOfKeyOtherThanKeyPrefix(byte[] key, byte[] keyPrefix, BytePredicate comparator) {
        if (key == null || keyPrefix == null) {
            return false;
        }
        int len = Math.min(key.length, keyPrefix.length);
        for (int i = 0; i < len; ++i) {
            if (key[i] != keyPrefix[i]) {
                return comparator.test(key[i], keyPrefix[i]);
            }
        }
        return false;
    }

    private static boolean keyStartsWithPrefix(byte[] key, byte[] prefix) {
        if (key == null || prefix == null) {
            return false;
        }
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; ++i) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static final BytePredicate GREATER = new BytePredicate() {
        @Override
        public boolean test(byte keyByte, byte prefixByte) {
            return ((keyByte & 0xff) - (prefixByte & 0xff)) > 0;
        }
    };

    private static final BytePredicate LESS = new BytePredicate() {
        @Override
        public boolean test(byte keyByte, byte prefixByte) {
            return ((keyByte & 0xff) - (prefixByte & 0xff)) < 0;
        }
    };

    private static interface BytePredicate {
        boolean test(byte keyByte, byte prefixByte);
    }
}
