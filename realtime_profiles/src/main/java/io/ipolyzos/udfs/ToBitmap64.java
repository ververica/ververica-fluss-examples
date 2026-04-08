package io.ipolyzos.udfs;


import java.io.IOException;

/** Converts a single BIGINT to a serialised single-element Roaring64Bitmap. */
public class ToBitmap64 extends BitmapUdf {
    public byte[] eval(Long value) throws IOException {
        if (value == null) {
            return null;
        }
        bitmapA.add(value);
        try {
            return serialize(bitmapA);
        } finally {
            bitmapA.clear();
        }
    }
}
