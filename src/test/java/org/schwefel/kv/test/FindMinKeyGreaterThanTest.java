package org.schwefel.kv.test;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.BiConsumer;

import org.schwefel.kv.ForEachKeyValue;
import org.schwefel.kv.KVStore;
import org.schwefel.kv.Kind;
import org.schwefel.kv.StoreOps;

public class FindMinKeyGreaterThanTest {

    static final byte[] value = { 5, 5, 5, 5 };
    static byte[] lowerBound = { 0, 0, 1, 0 };
    static byte[] keyPrefix = { 1 };
    //@formatter:off
    static final byte[][] keys = { { 0, 0, 0, 0 },
                                   { 0, 0, 0, 1 },
                                   { 0, 0, 1, 0 }, // lowerBound
                                   { 0, 0, 1, 1 },
                                   { 0, 0, 1, 2 },
                                   { 0, 0, 1, 3 },
                                   { 0, 0, 1, 4 },
                                   { 0, 0, 1, 5 },
                                   { 0, 0, 1, 6 },
                                   { 0, 1, 0, 0 },
                                   { 0, 1, 1, 1 },
                                   { 1, 1, 1, 1 }, // min
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
            try (ForEachKeyValue kv = store.scanAll(defaultKind)) {
                kv.forEachRemaining(new BiConsumer<byte[], byte[]>() {
                    @Override
                    public void accept(byte[] key, byte[] value) {
                        System.out.println(Arrays.toString(key) + " / " + Arrays.toString(value));
                    }
                });
            }

            System.out.println("smallest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly greater than " + Arrays.toString(lowerBound));
            byte[] min = store.findMinKeyGreaterThan(defaultKind, keyPrefix, lowerBound);
            System.out.println(Arrays.toString(min) + "\n");

            lowerBound = new byte[] { 4, 4, 4, 4 };
            System.out.println("smallest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly greater than " + Arrays.toString(lowerBound));
            min = store.findMinKeyGreaterThan(defaultKind, keyPrefix, lowerBound);
            System.out.println(Arrays.toString(min) + "\n");

            keyPrefix = new byte[] { 9, 9, 9, 9, 9 };
            System.out.println("smallest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly greater than " + Arrays.toString(lowerBound));
            min = store.findMinKeyGreaterThan(defaultKind, keyPrefix, lowerBound);
            System.out.println(Arrays.toString(min) + "\n");

            keyPrefix = new byte[] { 1 };
            lowerBound = new byte[] { 1, 2, 0, 1 };
            System.out.println("smallest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly greater than " + Arrays.toString(lowerBound));
            min = store.findMinKeyGreaterThan(defaultKind, keyPrefix, lowerBound);
            System.out.println(Arrays.toString(min) + "\n");

            keyPrefix = new byte[] { 0 };
            lowerBound = new byte[] { 0, 0, 0, 0 };
            System.out.println("smallest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly greater than " + Arrays.toString(lowerBound));
            min = store.findMinKeyGreaterThan(defaultKind, keyPrefix, lowerBound);
            System.out.println(Arrays.toString(min) + "\n");

            keyPrefix = new byte[] { 2 };
            lowerBound = new byte[] { 2, 1, 1, 1 };
            System.out.println("smallest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly greater than " + Arrays.toString(lowerBound));
            min = store.findMinKeyGreaterThan(defaultKind, keyPrefix, lowerBound);
            System.out.println(Arrays.toString(min) + "\n");

            lowerBound = new byte[] { 2, 2, 1, 1 };
            System.out.println("smallest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly greater than " + Arrays.toString(lowerBound));
            min = store.findMinKeyGreaterThan(defaultKind, keyPrefix, lowerBound);
            System.out.println(Arrays.toString(min) + "\n");

            keyPrefix = new byte[] { 0, 1, 0, 0 };
            lowerBound = new byte[] { 0, 1, 0, 1 };
            System.out.println("smallest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly greater than " + Arrays.toString(lowerBound));
            min = store.findMinKeyGreaterThan(defaultKind, keyPrefix, lowerBound);
            System.out.println(Arrays.toString(min) + "\n");
        }
    }
}
