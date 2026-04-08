package io.ipolyzos.udfs;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.*;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.roaringbitmap.longlong.LongIterator;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Converts a serialised Roaring64Bitmap to a human-readable string.
 *
 * Output format: "count=3 [1001, 1002, 1007]"
 *
 * If the bitmap has more than MAX_DISPLAY values, only the first MAX_DISPLAY are
 * listed and the result is suffixed with "…" so the output stays legible in a
 * SQL client terminal.
 *
 * Accepts both BINARY and VARBINARY (BYTES) input — Fluss rbm64 columns are
 * returned as BINARY, while UDF-produced bitmaps are VARBINARY/BYTES.
 */
public class BitmapToString extends BitmapUdf {

    private static final int MAX_DISPLAY = 20;

    public String eval(byte[] bitmap) throws Exception {
        if (bitmap == null) {
            return "null";
        }
        loadInto(bitmapA, bitmap);
        try {
            long cardinality = bitmapA.getLongCardinality();
            StringBuilder sb = new StringBuilder();
            sb.append("count=").append(cardinality).append(" [");
            LongIterator it = bitmapA.getLongIterator();
            int shown = 0;
            while (it.hasNext() && shown < MAX_DISPLAY) {
                if (shown > 0) sb.append(", ");
                sb.append(it.next());
                shown++;
            }
            if (cardinality > MAX_DISPLAY) {
                sb.append(", \u2026");
            }
            sb.append("]");
            return sb.toString();
        } finally {
            bitmapA.clear();
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
            .inputTypeStrategy(new InputTypeStrategy() {
                @Override
                public ArgumentCount getArgumentCount() {
                    return ConstantArgumentCount.of(1);
                }

                @Override
                public Optional<List<DataType>> inferInputTypes(
                        CallContext callContext, boolean throwOnFailure) {
                    List<DataType> args = callContext.getArgumentDataTypes();
                    if (args.size() != 1) {
                        return Optional.empty();
                    }
                    LogicalTypeRoot root = args.get(0).getLogicalType().getTypeRoot();
                    if (root == LogicalTypeRoot.BINARY || root == LogicalTypeRoot.VARBINARY) {
                        return Optional.of(Collections.singletonList(DataTypes.BYTES()));
                    }
                    if (throwOnFailure) {
                        throw callContext.newValidationError(
                            "bitmap_to_string expects a BINARY or BYTES argument.");
                    }
                    return Optional.empty();
                }

                @Override
                public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
                    return Collections.singletonList(
                        Signature.of(Signature.Argument.of("bitmap", "BYTES")));
                }
            })
            .outputTypeStrategy(TypeStrategies.explicit(DataTypes.STRING()))
            .build();
    }
}
