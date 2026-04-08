package io.ipolyzos.udfs;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

abstract class BitmapUdf extends ScalarFunction {

    // Two bitmap slots cover all operations: unary (bitmapA only) and binary (bitmapA op bitmapB).
    protected final Roaring64Bitmap bitmapA = new Roaring64Bitmap();
    protected final Roaring64Bitmap bitmapB = new Roaring64Bitmap();

    // transient: ByteArrayOutputStream is not Serializable. Flink serializes UDF instances
    // when shipping them to TaskManagers, so non-serializable fields must be transient and
    // re-initialized in open(), which is called on each subtask after deserialization.
    private transient ByteArrayOutputStream bos;
    private transient DataOutputStream dos;

    @Override
    public void open(FunctionContext context) throws Exception {
        bos = new ByteArrayOutputStream();
        dos = new DataOutputStream(bos);
    }

    /**
     * Serializes bm to bytes. Caller must ensure bm is cleared in a finally block
     * to match the server's post-use clear() convention in FieldRoaringBitmap64Agg.
     */
    protected byte[] serialize(Roaring64Bitmap bm) throws IOException {
        bm.runOptimize();
        bos.reset();
        bm.serialize(dos);
        return bos.toByteArray();
    }

    /**
     * Loads bytes into bm in-place. bm must be clear before this call (enforced by
     * the finally-block convention in each eval method below).
     */
    protected void loadInto(Roaring64Bitmap bm, byte[] bytes) throws IOException {
        bm.deserialize(ByteBuffer.wrap(bytes));
    }
}
