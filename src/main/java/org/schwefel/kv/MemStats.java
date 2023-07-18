/*
 * Copyright 2023 Stefan Zobel
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

import java.util.Map;
import java.util.TreeMap;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;

final class MemStats {

    // "rocksdb.size-all-mem-tables" - returns approximate size of active,
    // unflushed immutable, and pinned immutable memtables (bytes).
    private static final String SIZE_ALL_MEMTABLES = "rocksdb.size-all-mem-tables";

    // "rocksdb.cur-size-all-mem-tables" - returns approximate size of active
    // and unflushed immutable memtables (bytes).
    private static final String CURSIZE_ALL_MEMTABLES = "rocksdb.cur-size-all-mem-tables";

    // "rocksdb.cur-size-active-mem-table" - returns approximate size of active
    // memtable (bytes).
    private static final String CURSIZE_ACTIVE_MEMTABLE = "rocksdb.cur-size-active-mem-table";

    // "rocksdb.num-entries-active-mem-table" - returns total number of entries
    // in the active memtable.
    private static final String NUMENTRIES_ACTIVE_MEMTABLE = "rocksdb.num-entries-active-mem-table";

    // "rocksdb.num-immutable-mem-table" - returns number of immutable
    // memtables that have not yet been flushed.
    private static final String NUM_IMMUTABLE_MEMTABLE = "rocksdb.num-immutable-mem-table";

    // "rocksdb.num-entries-imm-mem-tables" - returns total number of entries
    // in the unflushed immutable memtables.
    private static final String NUMENTRIES_IMM_MEMTABLES = "rocksdb.num-entries-imm-mem-tables";

    // "rocksdb.estimate-table-readers-mem" - returns estimated memory used for
    // reading SST tables, excluding memory used in block cache (e.g.,
    // filter and index blocks).
    private static final String ESTIMATE_TABLEREADERS_MEM = "rocksdb.estimate-table-readers-mem";

    // "rocksdb.estimate-num-keys" - returns estimated number of total keys in
    // the active and unflushed immutable memtables and storage.
    private static final String ESTIMATE_NUMKEYS = "rocksdb.estimate-num-keys";

    private static final String[] KEYS = { SIZE_ALL_MEMTABLES, CURSIZE_ALL_MEMTABLES, CURSIZE_ACTIVE_MEMTABLE,
            NUMENTRIES_ACTIVE_MEMTABLE, NUM_IMMUTABLE_MEMTABLE, NUMENTRIES_IMM_MEMTABLES, ESTIMATE_TABLEREADERS_MEM,
            ESTIMATE_NUMKEYS };

    static Map<String, String> getStats(TransactionDB txnDb, KindImpl kind) {
        TreeMap<String, String> memStats = new TreeMap<>();
        ColumnFamilyHandle handle = kind.handle();
        for (String key : KEYS) {
            try {
                String value = txnDb.getProperty(handle, key);
                memStats.put(key, value);
            } catch (RocksDBException ignore) {
            }
        }
        return memStats;
    }
}
