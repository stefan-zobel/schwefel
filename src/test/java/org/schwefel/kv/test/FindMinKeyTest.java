package org.schwefel.kv.test;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.BiConsumer;

import org.schwefel.kv.ForEachKeyValue;
import org.schwefel.kv.KVStore;
import org.schwefel.kv.Kind;
import org.schwefel.kv.StoreOps;

public class FindMinKeyTest {

    private static final byte[] value = { 5, 5, 5, 5 };

    //@formatter:off
    static final byte[][] keys = { { 0, 0, 0, 0 },
                                   { 0, 0, 0, 1 },
                                   { 0, 0, 1, 0 },
                                   { 0, 0, 1, 1 },
                                   { 0, 0, 1, 2 },
                                   { 0, 0, 1, 3 },
                                   { 0, 0, 1, 4 },
                                   { 0, 0, 1, 5 },
                                   { 0, 0, 1, 6 },
                                   { 0, 1, 0, 0 },
                                   { 0, 1, 1, 1 },
                                   { 1, 1, 1, 1 },
                                   { 1, 1, 1, 2 },
                                   { 1, 2, 0, 1 },
                                   { 2, 1, 1, 1 },
                                   { 2, 2, 1, 1 }};
    //@formatter:on

    public static void main(String[] args) {

        try (StoreOps store = new KVStore(Paths.get("D:/Temp/rocksdb_database"))) {
            Kind defaultKind = store.getKindManagement().getDefaultKind();
            for (int i = 0; i < keys.length; ++i) {
                byte[] key = keys[i];
                store.put(defaultKind, key, value);
            }

            // retrieve in key order
            try (ForEachKeyValue kv = store.scanAll()) {
                kv.forEachRemaining(new BiConsumer<byte[], byte[]>() {
                    @Override
                    public void accept(byte[] key, byte[] value) {
                        System.out.println(Arrays.toString(key) + " / " + Arrays.toString(value));
                    }
                });
            }

            // min keys for several prefixes
            findMinKey(store);
        }
    }

    private static void findMinKey(StoreOps store) {
        byte[] keyPrefix = { 3 };
        byte[] key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] { 4, 4, 4, 4, 4 };
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] { 0, 0, 0, 0, 0 };
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] { 0, 0, 0, 0 };
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] { 0, 0, 0 };
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] { 0 };
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] {};
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] { 1 };
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] { 2 };
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] { 2, 2 };
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));

        keyPrefix = new byte[] { 1, 1 };
        key = store.findMinKey(keyPrefix);
        System.out.println("key: " + Arrays.toString(key) + " , prefix:" + Arrays.toString(keyPrefix));
    }
}
