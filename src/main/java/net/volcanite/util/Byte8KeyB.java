package net.volcanite.util;

import java.util.Objects;

/**
 * Key generator for auto-incrementing fixed length 8 byte[] array keys which
 * wrap-around on overflow. Note that the individual bytes in a key must be
 * interpreted as unsigned byte to get the correct lexicographic ordering. The
 * array representation of the underlying long is in big endian order.
 */
public final class Byte8KeyB {

    private static final byte[] MIN_KEY = putLongB(Long.MIN_VALUE, new byte[8]);
    private static final byte[] MAX_KEY = putLongB(Long.MAX_VALUE, new byte[8]);

    public static byte[] minKey() {
        return MIN_KEY.clone();
    }

    public static byte[] maxKey() {
        return MAX_KEY.clone();
    }

    private long curr;

    public Byte8KeyB() {
        curr = Long.MIN_VALUE;
    }

    public Byte8KeyB(long startAt) {
        curr = startAt;
    }

    /**
     * Construct a {@code Byte8Key} from a big endian byte array key.
     * 
     * @param key
     *            array in big endian representation
     */
    public Byte8KeyB(byte[] key) {
        if (Objects.requireNonNull(key).length != 8) {
            throw new IllegalArgumentException("wrong key length: " + key.length);
        }
        curr = getLongB(key);
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
        return putLongB(val, new byte[8]);
    }

    private static byte[] putLongB(long x, byte[] bytes) {
        bytes[0] = (byte) (x >> 56);
        bytes[1] = (byte) (x >> 48);
        bytes[2] = (byte) (x >> 40);
        bytes[3] = (byte) (x >> 32);
        bytes[4] = (byte) (x >> 24);
        bytes[5] = (byte) (x >> 16);
        bytes[6] = (byte) (x >> 8);
        bytes[7] = (byte) x;
        return bytes;
    }

    //@formatter:off
    private static long getLongB(byte[] bytes) {
        return makeLong(bytes[0],
                        bytes[1],
                        bytes[2],
                        bytes[3],
                        bytes[4],
                        bytes[5],
                        bytes[6],
                        bytes[7]);
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
