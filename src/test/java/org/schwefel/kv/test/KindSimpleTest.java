package org.schwefel.kv.test;

import java.nio.file.Paths;
import java.util.Set;

import org.schwefel.kv.KVStore;
import org.schwefel.kv.Kind;

public class KindSimpleTest {

    public static void main(String[] args) {

        try (KVStore store = new KVStore(Paths.get("D:/Temp/rocksdb_database"))) {
            Set<Kind> kinds = store.getKinds();
            System.out.println(kinds);

            Kind def = store.getKind("default");
            Kind not = store.getKind("does_not_exist");
            System.out.println(def);
            System.out.println(not);

            def = store.getOrCreateKind("default");
            System.out.println(def);

            Kind another = store.getOrCreateKind("another");
            System.out.println(another);

            kinds = store.getKinds();
            System.out.println(kinds);
        }
    }
}
