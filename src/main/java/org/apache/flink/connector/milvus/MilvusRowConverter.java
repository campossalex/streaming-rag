package org.apache.flink.connector.milvus;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.milvus.v2.service.vector.response.SearchResp;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Converts a Milvus search hit into Flink {@link RowData}.
 *
 * <p>The row VECTOR_SEARCH emits is every column of the search table followed by a {@code score}
 * column. The planner types that score column as DOUBLE, so it is written with
 * {@link GenericRowData#setField} as a boxed {@link Double} — emitting a Float here produces a
 * ClassCastException at runtime rather than a planning error.
 *
 * <p>Type support is deliberately limited to what a retrieval corpus needs. Anything else fails
 * fast with an actionable message instead of silently mis-converting.
 */
public class MilvusRowConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Converts one Milvus field value into its Flink internal representation. */
    private interface FieldConverter extends Serializable {
        Object convert(Object milvusValue);
    }

    private final String[] fieldNames;
    private final FieldConverter[] converters;
    private final int arity;

    public MilvusRowConverter(RowType rowType) {
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        this.arity = fieldNames.length;
        this.converters = new FieldConverter[arity];
        for (int i = 0; i < arity; i++) {
            converters[i] = createConverter(rowType.getTypeAt(i), fieldNames[i]);
        }
    }

    public RowData toRowData(SearchResp.SearchResult hit) {
        Map<String, Object> entity = hit.getEntity();
        // +1 for the trailing score column appended by VECTOR_SEARCH.
        GenericRowData row = new GenericRowData(arity + 1);
        for (int i = 0; i < arity; i++) {
            Object raw = entity == null ? null : entity.get(fieldNames[i]);
            if (raw == null && hit.getId() != null && entity != null && !entity.containsKey(fieldNames[i])) {
                // Milvus returns the primary key via getId() rather than in the entity map
                // depending on schema and requested output fields.
                raw = hit.getId();
            }
            row.setField(i, raw == null ? null : converters[i].convert(raw));
        }
        Float score = hit.getScore();
        row.setField(arity, score == null ? null : Double.valueOf(score.doubleValue()));
        return row;
    }

    private static FieldConverter createConverter(LogicalType type, String fieldName) {
        switch (type.getTypeRoot()) {
            case BIGINT:
                return v -> ((Number) v).longValue();
            case INTEGER:
                return v -> ((Number) v).intValue();
            case SMALLINT:
                return v -> ((Number) v).shortValue();
            case TINYINT:
                return v -> ((Number) v).byteValue();
            case FLOAT:
                return v -> ((Number) v).floatValue();
            case DOUBLE:
                return v -> ((Number) v).doubleValue();
            case BOOLEAN:
                return v -> (Boolean) v;
            case CHAR:
            case VARCHAR:
                return v -> StringData.fromString(String.valueOf(v));
            case ARRAY:
                return createArrayConverter((ArrayType) type, fieldName);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Field '%s' has type %s, which this Milvus connector does not "
                                        + "convert. Supported: BOOLEAN, TINYINT, SMALLINT, INT, "
                                        + "BIGINT, FLOAT, DOUBLE, CHAR/VARCHAR and ARRAY of the "
                                        + "numeric types. Cast or drop the column in the DDL.",
                                fieldName, type.asSummaryString()));
        }
    }

    private static FieldConverter createArrayConverter(ArrayType arrayType, String fieldName) {
        LogicalType element = arrayType.getElementType();
        switch (element.getTypeRoot()) {
            case FLOAT:
                return v -> {
                    List<?> values = asList(v, fieldName);
                    float[] out = new float[values.size()];
                    for (int i = 0; i < out.length; i++) {
                        out[i] = ((Number) values.get(i)).floatValue();
                    }
                    return new GenericArrayData(out);
                };
            case DOUBLE:
                return v -> {
                    List<?> values = asList(v, fieldName);
                    double[] out = new double[values.size()];
                    for (int i = 0; i < out.length; i++) {
                        out[i] = ((Number) values.get(i)).doubleValue();
                    }
                    return new GenericArrayData(out);
                };
            case INTEGER:
                return v -> {
                    List<?> values = asList(v, fieldName);
                    int[] out = new int[values.size()];
                    for (int i = 0; i < out.length; i++) {
                        out[i] = ((Number) values.get(i)).intValue();
                    }
                    return new GenericArrayData(out);
                };
            case BIGINT:
                return v -> {
                    List<?> values = asList(v, fieldName);
                    long[] out = new long[values.size()];
                    for (int i = 0; i < out.length; i++) {
                        out[i] = ((Number) values.get(i)).longValue();
                    }
                    return new GenericArrayData(out);
                };
            case CHAR:
            case VARCHAR:
                return v -> {
                    List<?> values = asList(v, fieldName);
                    StringData[] out = new StringData[values.size()];
                    for (int i = 0; i < out.length; i++) {
                        out[i] = StringData.fromString(String.valueOf(values.get(i)));
                    }
                    return new GenericArrayData(out);
                };
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Field '%s' is an ARRAY of %s, which this connector does not convert.",
                                fieldName, element.asSummaryString()));
        }
    }

    private static List<?> asList(Object value, String fieldName) {
        if (value instanceof List) {
            return (List<?>) value;
        }
        throw new IllegalStateException(
                String.format(
                        "Expected Milvus to return field '%s' as a List but got %s. If this is a "
                                + "vector field, check that it is declared ARRAY<FLOAT> in the DDL.",
                        fieldName, value.getClass().getName()));
    }
}
