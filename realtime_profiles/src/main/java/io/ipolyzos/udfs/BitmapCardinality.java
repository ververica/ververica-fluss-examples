package io.ipolyzos.udfs;

/** Returns the exact cardinality of the serialised bitmap. */
public class BitmapCardinality extends BitmapUdf {
    public Long eval(byte[] bitmap) throws Exception {
        if (bitmap == null) {
            return 0L;
        }
        loadInto(bitmapA, bitmap);
        try {
            return bitmapA.getLongCardinality();
        } finally {
            bitmapA.clear();
        }
    }
}