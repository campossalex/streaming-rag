package org.apache.flink.connector.milvus;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.connector.source.search.AsyncVectorSearchFunctionProvider;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** A Milvus collection exposed to Flink SQL's VECTOR_SEARCH table-valued function. */
public class MilvusVectorSearchTableSource implements VectorSearchTableSource {

    /**
     * The VECTOR_SEARCH CONFIG map is surfaced to the connector through
     * {@code VectorSearchContext#runtimeConfig()}. Reading the raw key keeps this class independent
     * of flink-table-api-java.
     */
    private static final ConfigOption<Boolean> ASYNC =
            ConfigOptions.key("async").booleanType().defaultValue(false);

    private final MilvusConfig config;
    private final RowType physicalRowType;

    public MilvusVectorSearchTableSource(MilvusConfig config, RowType physicalRowType) {
        this.config = config;
        this.physicalRowType = physicalRowType;
    }

    @Override
    public VectorSearchRuntimeProvider getSearchRuntimeProvider(VectorSearchContext context) {
        String annsField = resolveAnnsField(context);
        // Milvus must return every column the table declares, because VECTOR_SEARCH emits the full
        // search-table row plus score. Requesting the vector column back costs bandwidth; drop it
        // from the DDL if the query never selects it.
        List<String> outputFields = physicalRowType.getFieldNames();
        MilvusRowConverter converter = new MilvusRowConverter(physicalRowType);

        if (context.runtimeConfig().get(ASYNC)) {
            return AsyncVectorSearchFunctionProvider.of(
                    new MilvusAsyncVectorSearchFunction(
                            config, converter, annsField, outputFields));
        }
        return VectorSearchFunctionProvider.of(
                new MilvusVectorSearchFunction(config, converter, annsField, outputFields));
    }

    /**
     * Turns the {@code DESCRIPTOR(...)} column reference into the Milvus {@code annsField} name.
     * Search columns arrive as index paths into the row type; a vector column is always top level,
     * so the path has exactly one element.
     */
    private String resolveAnnsField(VectorSearchContext context) {
        int[][] searchColumns = context.getSearchColumns();
        if (searchColumns.length != 1 || searchColumns[0].length != 1) {
            throw new IllegalArgumentException(
                    "VECTOR_SEARCH on Milvus requires exactly one top-level vector column in "
                            + "DESCRIPTOR(...). Nested or composite search columns are not supported.");
        }
        int index = searchColumns[0][0];
        List<String> fieldNames = physicalRowType.getFieldNames();
        if (index < 0 || index >= fieldNames.size()) {
            throw new IllegalStateException(
                    "Search column index " + index + " is outside the table schema.");
        }
        return fieldNames.get(index);
    }

    @Override
    public DynamicTableSource copy() {
        return new MilvusVectorSearchTableSource(config, physicalRowType);
    }

    @Override
    public String asSummaryString() {
        return "Milvus[" + config.getCollectionName() + "]";
    }
}
