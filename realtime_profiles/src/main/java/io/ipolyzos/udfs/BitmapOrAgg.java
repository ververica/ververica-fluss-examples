package io.ipolyzos.udfs;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Aggregate function that ORs multiple serialised Roaring64Bitmaps into a single bitmap.
 * Required when querying across group keys that share a common prefix
 * (e.g. 'high_risk_jurisdiction:%') to avoid a Cartesian product in the FROM clause.
 *
 * Note: OR aggregation is not retractable. Use only in append or tumbling-window contexts.
 *
 * Acc stores byte[] rather than Roaring64Bitmap so that Flink's POJO serializer
 * only sees primitive-friendly types. Roaring64Bitmap has internal fields
 * (highLowContainer) that are not publicly accessible and would cause
 * a ValidationException when Flink tries to introspect the Acc class.
 */
public class BitmapOrAgg extends AggregateFunction<byte[], BitmapOrAgg.Acc> {

    // Reusable bitmaps for each accumulate() / getValue() call.
    // Safe as member fields: each Flink subtask has its own UDF instance.
    private final Roaring64Bitmap incomingBitmap = new Roaring64Bitmap();
    private final Roaring64Bitmap runningBitmap  = new Roaring64Bitmap();

    // transient: ByteArrayOutputStream is not Serializable.
    private transient ByteArrayOutputStream bos;
    private transient DataOutputStream dos;

    @Override
    public void open(FunctionContext context) throws Exception {
        bos = new ByteArrayOutputStream();
        dos = new DataOutputStream(bos);
    }

    public static class Acc {
        // Serialised OR result. null means no value has been accumulated yet.
        // byte[] and boolean are POJO-safe — Flink's type system handles them natively.
        public byte[]  bitmapBytes = null;
        public boolean hasValue    = false;
    }

    @Override
    public Acc createAccumulator() { return new Acc(); }

    /**
     * No-op retract. Bitmap OR is not mathematically reversible, but it is safe to ignore
     * retractions here because risk_groups uses the rbm64 aggregation merge engine, which
     * only ever ORs bits in — it never removes them. A retracted row's bitmap is always a
     * subset of the replacement row's bitmap, so the accumulated OR result is unaffected.
     */
    public void retract(Acc acc, byte[] value) {
        // intentionally empty
    }

    public void accumulate(Acc acc, byte[] value) throws IOException {
        if (value == null) {
            return;
        }
        incomingBitmap.deserialize(ByteBuffer.wrap(value));
        try {
            if (acc.bitmapBytes == null) {
                // First value for this group: store it directly.
                acc.bitmapBytes = serialize(incomingBitmap);
            } else {
                // Subsequent value: OR into the accumulated bitmap.
                runningBitmap.deserialize(ByteBuffer.wrap(acc.bitmapBytes));
                try {
                    runningBitmap.or(incomingBitmap);
                    acc.bitmapBytes = serialize(runningBitmap);
                } finally {
                    runningBitmap.clear();
                }
            }
            acc.hasValue = true;
        } finally {
            incomingBitmap.clear();
        }
    }

    @Override
    public byte[] getValue(Acc acc) {
        if (!acc.hasValue || acc.bitmapBytes == null) {
            return null;
        }
        // Re-deserialize, optimize, and re-serialize so the output bitmap is run-compressed.
        try {
            runningBitmap.deserialize(ByteBuffer.wrap(acc.bitmapBytes));
            runningBitmap.runOptimize();
            bos.reset();
            runningBitmap.serialize(dos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            runningBitmap.clear();
        }
    }

    private byte[] serialize(Roaring64Bitmap bm) throws IOException {
        bos.reset();
        bm.serialize(dos);
        return bos.toByteArray();
    }
}
