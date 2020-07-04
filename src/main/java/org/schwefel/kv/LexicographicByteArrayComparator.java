/*
 * Copyright 2020 Stefan Zobel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schwefel.kv;

import java.util.Comparator;

public class LexicographicByteArrayComparator implements Comparator<byte[]> {

    public static final LexicographicByteArrayComparator COMPARATOR = new LexicographicByteArrayComparator();

    /**
     * {@inheritDoc}
     */
    @Override
    public int compare(byte[] a, byte[] b) {
        return lexicographicalCompare(a, b);
    }

    /**
     * Compares two {@code byte} arrays lexicographically.
     * 
     * @param a
     *            the first array to compare
     * @param b
     *            the second array to compare
     * @return the value {@code 0} if the first and second array are equal and
     *         contain the same elements in the same order; a value less than
     *         {@code 0} if the first array is lexicographically less than the
     *         second array; and a value greater than {@code 0} if the first
     *         array is lexicographically greater than the second array
     */
    public static int lexicographicalCompare(byte[] a, byte[] b) {
        if (a == b) {
            return 0;
        }
        if (a == null || b == null) {
            return a == null ? -1 : 1;
        }
        int i = mismatch(a, b, Math.min(a.length, b.length));
        if (i >= 0) {
            return Byte.compare(a[i], b[i]);
        }
        return a.length - b.length;
    }

    private static int mismatch(byte[] a, byte[] b, int length) {
        for (int i = 0; i < length; ++i) {
            if (a[i] != b[i]) {
                return i;
            }
        }
        return -1;
    }
}
