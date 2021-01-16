package org.schwefel.kv.test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;

import org.schwefel.kv.ForEachKeyValue;
import org.schwefel.kv.KVStore;
import org.schwefel.kv.StoreOps;

import net.volcanite.util.Byte4Key;

public class ScanTest {

    public static void main(String[] args) {
        int DOC_COUNT = 301;
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
            for (int i = 0; i < keys.size(); ++i) {
                byte[] key = keys.get(i);
                System.out.println("store key : " + getIntB(key));
                store.put(key, values.get(i));
            }

            // retrieve in key order (= reversed storage order)
            try (ForEachKeyValue kv = store.scanAll(Byte4Key.minKey())) {
                kv.forEachRemaining(new BiConsumer<byte[], byte[]>() {
                    @Override
                    public void accept(byte[] key, byte[] value) {
                        System.out.println(new String(value) + " / " + Arrays.toString(key));
                    }
                });
            }
        }
    }

    //@formatter:off
    private static int getIntB(byte[] bytes) {
        return makeInt(bytes[0],
                       bytes[1],
                       bytes[2],
                       bytes[3]);
    }
    //@formatter:on

    //@formatter:off
    private static int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }
    //@formatter:off
}
