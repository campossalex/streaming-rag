package org.apache.flink.connector.milvus;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import io.milvus.v2.service.vector.response.SearchResp;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MilvusRowConverterTest {

    private static RowType workshopSchema() {
        return RowType.of(
                new LogicalType[] {
                    new BigIntType(false),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new ArrayType(new FloatType())
                },
                new String[] {"id", "content", "category", "vec"});
    }

    private static SearchResp.SearchResult hit() {
        Map<String, Object> entity = new HashMap<>();
        entity.put("id", 7L);
        entity.put("content", "Defective units are covered by a 24-month warranty.");
        entity.put("category", "warranty");
        entity.put("vec", Arrays.asList(0.1f, 0.2f, 0.3f));
        return SearchResp.SearchResult.builder().entity(entity).score(0.8734f).id(7L).build();
    }

    @Test
    void convertsAllColumnsAndAppendsScore() {
        RowData row = new MilvusRowConverter(workshopSchema()).toRowData(hit());

        assertEquals(5, row.getArity(), "4 table columns plus the trailing score column");
        assertEquals(7L, row.getLong(0));
        assertEquals("warranty", row.getString(2).toString());
        assertEquals(3, row.getArray(3).size());
        assertEquals(0.8734f, row.getDouble(4), 1e-6);
    }

    /**
     * The planner types the score column as DOUBLE. Writing a Float would compile fine and then
     * fail deep in generated code at runtime, so pin the behaviour here.
     */
    @Test
    void scoreIsDoubleNotFloat() {
        RowData row = new MilvusRowConverter(workshopSchema()).toRowData(hit());
        assertThrows(ClassCastException.class, () -> row.getFloat(4));
    }

    @Test
    void rejectsUnsupportedColumnTypeWithActionableMessage() {
        RowType withMap =
                RowType.of(
                        new LogicalType[] {
                            new org.apache.flink.table.types.logical.MapType(
                                    new VarCharType(VarCharType.MAX_LENGTH),
                                    new VarCharType(VarCharType.MAX_LENGTH)),
                            new ArrayType(new FloatType())
                        },
                        new String[] {"meta", "vec"});
        assertThrows(
                UnsupportedOperationException.class, () -> new MilvusRowConverter(withMap));
    }
}
