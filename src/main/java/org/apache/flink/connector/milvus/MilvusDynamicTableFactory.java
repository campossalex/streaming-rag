package org.apache.flink.connector.milvus;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Set;

/** Registers {@code 'connector' = 'milvus'} for vector-search source tables. */
public class MilvusDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "milvus";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MilvusOptions.COLLECTION_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MilvusOptions.ENDPOINT);
        options.add(MilvusOptions.PORT);
        options.add(MilvusOptions.URI);
        options.add(MilvusOptions.DATABASE_NAME);
        options.add(MilvusOptions.USER_NAME);
        options.add(MilvusOptions.PASSWORD);
        options.add(MilvusOptions.TOKEN);
        options.add(MilvusOptions.SEARCH_METRIC);
        options.add(MilvusOptions.SEARCH_CONSISTENCY_LEVEL);
        options.add(MilvusOptions.SEARCH_FILTER);
        options.add(MilvusOptions.QUERY_VECTOR_TYPE);
        options.add(MilvusOptions.SEARCH_MAX_RETRIES);
        options.add(MilvusOptions.CONNECT_TIMEOUT);
        options.add(MilvusOptions.ASYNC_THREAD_POOL_SIZE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();

        RowType physicalRowType =
                (RowType)
                        context.getPhysicalRowDataType().getLogicalType();
        validateHasVectorColumn(physicalRowType);

        return new MilvusVectorSearchTableSource(new MilvusConfig(options), physicalRowType);
    }

    /** Fail at planning time rather than with a confusing Milvus error at runtime. */
    private static void validateHasVectorColumn(RowType rowType) {
        for (LogicalType type : rowType.getChildren()) {
            if (type.getTypeRoot() == LogicalTypeRoot.ARRAY) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "A Milvus vector-search table needs at least one ARRAY<FLOAT> column to use as the "
                        + "DESCRIPTOR(...) search column, but the schema has none.");
    }
}
