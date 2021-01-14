package net.volcanite.util;

import java.util.Objects;

/**
 * Key generator for auto-incrementing fixed length 4 byte[] array keys which
 * wrap-around on overflow. Note that the individual bytes in a key must be
 * interpreted as unsigned byte to get the correct lexicographic ordering. The
 * array representation of the underlying int is in big endian order.
 */
public final class Byte4KeyB {

    private static final byte[] MIN_KEY = putIntB(Integer.MIN_VALUE, new byte[4]);
    private static final byte[] MAX_KEY = putIntB(Integer.MAX_VALUE, new byte[4]);

    public static byte[] minKey() {
        return MIN_KEY.clone();
    }

    public static byte[] maxKey() {
        return MAX_KEY.clone();
    }

    private int curr;

    public Byte4KeyB() {
        curr = Integer.MIN_VALUE;
    }

    public Byte4KeyB(int startAt) {
        curr = startAt;
    }

    /**
     * Construct a {@code Byte4Key} from a big endian byte array key.
     * 
     * @param key
     *            array in big endian representation
     */
    public Byte4KeyB(byte[] key) {
        if (Objects.requireNonNull(key).length != 4) {
            throw new IllegalArgumentException("wrong key length: " + key.length);
        }
        curr = getIntB(key);
    }

    public byte[] next() {
        return create(curr++);
    }

    public byte[] current() {
        return create(curr);
    }

    public int currentValue() {
        return curr;
    }

    private static byte[] create(int val) {
        return putIntB(val, new byte[4]);
    }

    private static byte[] putIntB(int x, byte[] bytes) {
        bytes[0] = (byte) (x >> 24);
        bytes[1] = (byte) (x >> 16);
        bytes[2] = (byte) (x >> 8);
        bytes[3] = (byte) x;
        return bytes;
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
