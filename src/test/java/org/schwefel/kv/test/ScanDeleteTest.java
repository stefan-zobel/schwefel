package org.schwefel.kv.test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;

import org.schwefel.kv.BasicOps;
import org.schwefel.kv.ForEachKeyValue;
import org.schwefel.kv.KVStore;
import org.schwefel.kv.Kind;
import org.schwefel.kv.StoreOps;

import net.volcanite.util.Byte4Key;

public class ScanDeleteTest {

    public static void main(String[] args) {
        int DOC_COUNT = 257;
        Byte4Key gen = new Byte4Key();

        int val = 0;
        ArrayList<byte[]> keys = new ArrayList<>(DOC_COUNT);
        ArrayList<byte[]> values = new ArrayList<>(DOC_COUNT);

        for (int i = 0; i < DOC_COUNT; ++i) {
            keys.add(gen.next());
            values.add(String.valueOf(val++).getBytes());
        }
        // store in reverse order
        Collections.reverse(keys);
        Collections.reverse(values);

        try (StoreOps store = new KVStore(Paths.get("D:/Temp/rocksdb_database"))) {
            Kind defaultKind = store.getKindManagement().getDefaultKind();
            for (int i = 0; i < keys.size(); ++i) {
                store.put(defaultKind, keys.get(i), values.get(i));
            }

            // retrieve in key order (= reversed storage order)
            try (ForEachKeyValue kv = store.scanAll(defaultKind)) {
                BasicOps ops = kv.ops();
                kv.forEachRemaining(new BiConsumer<byte[], byte[]>() {
                    @Override
                    public void accept(byte[] key, byte[] value) {
                        String stringVal = new String(value);
                        System.out.println(stringVal + " / " + Arrays.toString(key));
                        if (stringVal.length() == 2 && Integer.parseInt(stringVal) % 2 == 0) {
                            System.out.println("Deleting value " + stringVal);
                            ops.delete(defaultKind, key);
                        }
                    }
                });
            }
            System.out.println("++++++ REFETCH ++++++");
            // check that docs have been deleted
            try (ForEachKeyValue kv = store.scanAll(defaultKind)) {
                kv.forEachRemaining(new BiConsumer<byte[], byte[]>() {
                    @Override
                    public void accept(byte[] key, byte[] value) {
                        System.out.println(new String(value) + " / " + Arrays.toString(key));
                    }
                });
            }
        }
    }
}
