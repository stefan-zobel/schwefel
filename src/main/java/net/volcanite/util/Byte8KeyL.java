package net.volcanite.util;

import java.util.Objects;

/**
 * Key generator for auto-incrementing fixed length 8 byte[] array keys which
 * wrap-around on overflow. Note that the individual bytes in a key must be
 * interpreted as unsigned byte to get the correct lexicographic ordering. The
 * array representation of the underlying long is in little endian order.
 */
public final class Byte8KeyL {

    private static final byte[] MIN_KEY = putLongL(Long.MIN_VALUE, new byte[8]);
    private static final byte[] MAX_KEY = putLongL(Long.MAX_VALUE, new byte[8]);

    public static byte[] minKey() {
        return MIN_KEY.clone();
    }

    public static byte[] maxKey() {
        return MAX_KEY.clone();
    }

    private long curr;

    public Byte8KeyL() {
        curr = Long.MIN_VALUE;
    }

    public Byte8KeyL(long startAt) {
        curr = startAt;
    }

    /**
     * Construct a {@code Byte8KeyL} from a little endian byte array key.
     * 
     * @param key
     *            array in little endian representation
     */
    public Byte8KeyL(byte[] key) {
        if (Objects.requireNonNull(key).length != 8) {
            throw new IllegalArgumentException("wrong key length: " + key.length);
        }
        curr = getLongL(key);
    }

    public byte[] next() {
        return create(curr++);
    }

    public byte[] current() {
        return create(curr);
    }

    public long currentValue() {
        return curr;
    }

    private static byte[] create(long val) {
        return putLongL(val, new byte[8]);
    }

    private static byte[] putLongL(long x, byte[] bytes) {
        bytes[7] = (byte) (x >> 56);
        bytes[6] = (byte) (x >> 48);
        bytes[5] = (byte) (x >> 40);
        bytes[4] = (byte) (x >> 32);
        bytes[3] = (byte) (x >> 24);
        bytes[2] = (byte) (x >> 16);
        bytes[1] = (byte) (x >> 8);
        bytes[0] = (byte) x;
        return bytes;
    }

    //@formatter:off
    private static long getLongL(byte[] bytes) {
        return makeLong(bytes[7],
                        bytes[6],
                        bytes[5],
                        bytes[4],
                        bytes[3],
                        bytes[2],
                        bytes[1],
                        bytes[0]);
    }
    //@formatter:on

    //@formatter:off
    private static long makeLong(byte b7, byte b6, byte b5, byte b4,
            byte b3, byte b2, byte b1, byte b0) {
        return ((((long) b7       ) << 56) |
                (((long) b6 & 0xff) << 48) |
                (((long) b5 & 0xff) << 40) |
                (((long) b4 & 0xff) << 32) |
                (((long) b3 & 0xff) << 24) |
                (((long) b2 & 0xff) << 16) |
                (((long) b1 & 0xff) <<  8) |
                (((long) b0 & 0xff)      ));
    }
    //@formatter:on
}
