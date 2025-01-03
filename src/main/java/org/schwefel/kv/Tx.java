/*
 * Copyright 2020, 2022 Stefan Zobel
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

public interface Tx extends BasicOps, AutoCloseable {

    void commit();
    void rollback();
    void close();
    void disableIndexing();
    void enableIndexing();
    void setLockTimeout(long lockTimeoutMillis);
    void put(Kind kind, byte[] key, byte[] value);
    void putIfAbsent(Kind kind, byte[] key, byte[] value);
    byte[] get(Kind kind, byte[] key);
    List<byte[]> multiGet(List<Kind> kinds, List<byte[]> keys);
    byte[] getForUpdate(Kind kind, byte[] key);
    byte[] getForUpdate(Kind kind, byte[] key, boolean exclusive);
    List<byte[]> multiGetForUpdate(List<Kind> kinds, List<byte[]> keys);
    void undoGetForUpdate(Kind kind, byte[] key);
    void delete(Kind kind, byte[] key);
    byte[] deleteIfPresent(Kind kind, byte[] key);
    byte[] updateIfPresent(Kind kind, byte[] key, byte[] value);
    void singleDelete(Kind kind, byte[] key);
    byte[] singleDeleteIfPresent(Kind kind, byte[] key);
    ForEachKeyValue scanAll(Kind kind);
    ForEachKeyValue scanAll(Kind kind, byte[] beginKey);
    ForEachKeyValue scanRange(Kind kind, byte[] beginKey, byte[] endKey);
    byte[] findMinKey(Kind kind);
    byte[] findMinKeyByPrefix(Kind kind, byte[] keyPrefix);
    byte[] findMinKeyByLowerBound(Kind kind, byte[] lowerBound);
    byte[] findMinKeyGreaterThan(Kind kind, byte[] keyPrefix, byte[] lowerBound);
    byte[] findMaxKey(Kind kind);
    byte[] findMaxKeyByPrefix(Kind kind, byte[] keyPrefix);
    byte[] findMaxKeyByUpperBound(Kind kind, byte[] upperBound);
    byte[] findMaxKeyLessThan(Kind kind, byte[] keyPrefix, byte[] upperBound);
}
