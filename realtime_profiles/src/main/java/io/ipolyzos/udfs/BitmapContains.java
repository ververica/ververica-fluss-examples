package io.ipolyzos.udfs;

/** Returns true if the serialised bitmap contains the given BIGINT. */
public class BitmapContains extends BitmapUdf {
    public Boolean eval(byte[] bitmap, Long value) throws Exception {
        if (bitmap == null || value == null) {
            return false;
        }
        loadInto(bitmapA, bitmap);
        try {
            return bitmapA.contains(value);
        } finally {
            bitmapA.clear();
        }
    }
}