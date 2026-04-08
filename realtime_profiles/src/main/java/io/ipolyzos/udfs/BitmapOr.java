package io.ipolyzos.udfs;

/** Returns the union (OR) of two serialised bitmaps. */
public class BitmapOr extends BitmapUdf {
    public byte[] eval(byte[] a, byte[] b) throws Exception {
        if (a == null) {
            return b;
        }

        if (b == null) {
            return a;
        }

        loadInto(bitmapA, a);
        loadInto(bitmapB, b);
        try {
            bitmapA.or(bitmapB);
            return serialize(bitmapA);
        } finally {
            bitmapA.clear();
            bitmapB.clear();
        }
    }
}