package org.schwefel.kv.test;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.BiConsumer;

import org.schwefel.kv.ForEachKeyValue;
import org.schwefel.kv.KVStore;
import org.schwefel.kv.Kind;
import org.schwefel.kv.StoreOps;

public class FindMaxKeyLessThanTest {

    static final byte[] value = { 5, 5, 5, 5 };
    static byte[] upperBound = { 2, 1, 1, 1 };
    static byte[] keyPrefix = { 0 };
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
                                   { 0, 1, 1, 1 }, // max
                                   { 1, 1, 1, 1 },
                                   { 1, 1, 1, 2 },
                                   { 1, 2, 0, 1 },
                                   { 2, 1, 1, 1 }, // upperBound
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

            System.out.println("largest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly less than " + Arrays.toString(upperBound));
            byte[] max = store.findMaxKeyLessThan(defaultKind, keyPrefix, upperBound);
            System.out.println(Arrays.toString(max) + "\n");

            keyPrefix = new byte[] { 3 };
            System.out.println("largest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly less than " + Arrays.toString(upperBound));
            max = store.findMaxKeyLessThan(defaultKind, keyPrefix, upperBound);
            System.out.println(Arrays.toString(max) + "\n");

            keyPrefix = new byte[] { 9, 9, 9, 9, 9 };
            upperBound = new byte[] { 0, 0, 0, 0 };
            System.out.println("largest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly less than " + Arrays.toString(upperBound));
            max = store.findMaxKeyLessThan(defaultKind, keyPrefix, upperBound);
            System.out.println(Arrays.toString(max) + "\n");

            keyPrefix = new byte[] { 2 };
            upperBound = new byte[] { 2, 1, 1, 1 };
            System.out.println("largest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly less than " + Arrays.toString(upperBound));
            max = store.findMaxKeyLessThan(defaultKind, keyPrefix, upperBound);
            System.out.println(Arrays.toString(max) + "\n");

            upperBound = new byte[] { 2, 2, 1, 1 };
            System.out.println("largest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly less than " + Arrays.toString(upperBound));
            max = store.findMaxKeyLessThan(defaultKind, keyPrefix, upperBound);
            System.out.println(Arrays.toString(max) + "\n");

            keyPrefix = new byte[] { 0 };
            upperBound = new byte[] { 0, 0, 0, 1 };
            System.out.println("largest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly less than " + Arrays.toString(upperBound));
            max = store.findMaxKeyLessThan(defaultKind, keyPrefix, upperBound);
            System.out.println(Arrays.toString(max) + "\n");

            keyPrefix = new byte[] { 1 };
            upperBound = new byte[] { 4, 4, 4, 4 };
            System.out.println("largest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly less than " + Arrays.toString(upperBound));
            max = store.findMaxKeyLessThan(defaultKind, keyPrefix, upperBound);
            System.out.println(Arrays.toString(max) + "\n");

            upperBound = new byte[] { 0, 1, 0, 0 };
            keyPrefix = new byte[] { 0, 1, 0, 1 };
            System.out.println("largest key starting with prefix " + Arrays.toString(keyPrefix)
                    + " being strictly less than " + Arrays.toString(upperBound));
            max = store.findMaxKeyLessThan(defaultKind, keyPrefix, upperBound);
            System.out.println(Arrays.toString(max) + "\n");
        }
    }
}
