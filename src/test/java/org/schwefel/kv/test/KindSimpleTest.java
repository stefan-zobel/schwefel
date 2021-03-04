package org.schwefel.kv.test;

import java.nio.file.Paths;
import java.util.Set;

import org.schwefel.kv.KVStore;
import org.schwefel.kv.Kind;
import org.schwefel.kv.StoreOps;

public class KindSimpleTest {

    public static void main(String[] args) {

        try (StoreOps store = new KVStore(Paths.get("D:/Temp/rocksdb_database"))) {
            Set<Kind> kinds = store.getKindManagement().getKinds();
            System.out.println(kinds);

            Kind def = store.getKindManagement().getKind("default");
            Kind not = store.getKindManagement().getKind("does_not_exist");
            System.out.println(def);
            System.out.println(not);

            def = store.getKindManagement().getOrCreateKind("default");
            System.out.println(def);

            Kind another = store.getKindManagement().getOrCreateKind("another");
            System.out.println(another);
            another = store.getKindManagement().getOrCreateKind("another");
            System.out.println(another);

            kinds = store.getKindManagement().getKinds();
            System.out.println(kinds);
            def = store.getKindManagement().getDefaultKind();
            System.out.println(def);
        }
    }
}
