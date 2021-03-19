package net.volcanite.util;

public final class Hash {

    public static int hash32(int h) {
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);
        return h ^ (h >>> 16);
    }

    private Hash() {
        throw new AssertionError();
    }
}
