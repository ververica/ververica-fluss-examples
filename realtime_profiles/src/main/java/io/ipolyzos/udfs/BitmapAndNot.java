package io.ipolyzos.udfs;

/**
 * Returns the difference (AND NOT) of two serialised bitmaps.
 *
 * Null semantics: a=null → null; b=null → a.
 * Observation: both null cases return a (when a is null, returning a IS returning null),
 * so the two guards unify: if (a == null || b == null) return a.
 */
public class BitmapAndNot extends BitmapUdf {
    public byte[] eval(byte[] a, byte[] b) throws Exception {
        if (a == null || b == null) {
            return a;
        }

        loadInto(bitmapA, a);
        loadInto(bitmapB, b);
        try {
            bitmapA.andNot(bitmapB);
            return serialize(bitmapA);
        } finally {
            bitmapA.clear();
            bitmapB.clear();
        }
    }
}