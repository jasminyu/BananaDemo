package com.etisalat.log.sort;

import org.apache.lucene.util.BytesRef;

import java.util.Comparator;

public class StringComparator implements Comparator<BytesRef> {
    // Only singleton
    StringComparator() {
    }

    ;

    @Override
    public int compare(BytesRef a, BytesRef b) {
        final byte[] aBytes = a.bytes;
        int aUpto = a.offset;
        final byte[] bBytes = b.bytes;
        int bUpto = b.offset;

        final int aStop = aUpto + Math.min(a.length, b.length);
        while (aUpto < aStop) {
            int aByte = aBytes[aUpto++] & 0xff;
            int bByte = bBytes[bUpto++] & 0xff;

            int diff = aByte - bByte;
            if (diff != 0) {
                return diff;
            }
        }

        // One is a prefix of the other, or, they are equal:
        return a.length - b.length;
    }
}
