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

import org.rocksdb.RocksIterator;

import static org.schwefel.kv.LexicographicByteArrayComparator.lexicographicalCompare;

final class MinMaxKeyIt {

    public static void main(String[] args) {

        // prefix(key) >= keyPrefix

        // prefix(key) < keyPrefix
    }

    static byte[] findMinKeyGreaterThan(RocksIterator iter, Stats stats, byte[] keyPrefix, byte[] lowerBound) {
        try {
            if (iter.isOwningHandle()) {
                iter.seek(lowerBound);
                byte[] key = null;
                if (iter.isValid() && (key = iter.key()) != null && lexicographicalCompare(key, lowerBound) == 0) {
                    iter.next();
                }
                while (iter.isValid() && (key = iter.key()) != null && prefixOfKeyLessThanKeyPrefix(key, keyPrefix)) {
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

    private static boolean prefixOfKeyLessThanKeyPrefix(byte[] key, byte[] keyPrefix) {
        if (key == null || keyPrefix == null) {
            return false;
        }
        int len = Math.min(key.length, keyPrefix.length);
        for (int i = 0; i < len; ++i) {
            if (key[i] != keyPrefix[i]) {
                return ((key[i] & 0xff) - (keyPrefix[i] & 0xff)) < 0;
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
}
