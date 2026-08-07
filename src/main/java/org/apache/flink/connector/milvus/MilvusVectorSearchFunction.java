package org.apache.flink.connector.milvus;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.VectorSearchFunction;

import io.milvus.v2.service.vector.response.SearchResp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Synchronous top-k retrieval against Milvus.
 *
 * <p>{@code VectorSearchFunction} hands us the probe vector wrapped in a single-field RowData, and
 * expects at most topK rows back.
 */
public class MilvusVectorSearchFunction extends VectorSearchFunction {

    private static final long serialVersionUID = 1L;

    private final MilvusConfig config;
    private final MilvusRowConverter converter;
    private final String annsField;
    private final List<String> outputFields;

    private transient MilvusSearchClient client;

    public MilvusVectorSearchFunction(
            MilvusConfig config,
            MilvusRowConverter converter,
            String annsField,
            List<String> outputFields) {
        this.config = config;
        this.converter = converter;
        this.annsField = annsField;
        this.outputFields = outputFields;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        client = new MilvusSearchClient(config, annsField, outputFields);
        client.open();
    }

    @Override
    public Collection<RowData> vectorSearch(int topK, RowData queryData) throws IOException {
        if (queryData == null || queryData.isNullAt(0)) {
            // A null probe matches nothing; the planner turns this into zero rows for an inner
            // correlate and a single null-padded row for a LEFT JOIN LATERAL.
            return Collections.emptyList();
        }
        float[] probe = toFloatArray(queryData.getArray(0), config.isQueryVectorDouble());
        List<SearchResp.SearchResult> hits = client.search(probe, topK);
        List<RowData> rows = new ArrayList<>(hits.size());
        for (SearchResp.SearchResult hit : hits) {
            rows.add(converter.toRowData(hit));
        }
        return rows;
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
            client = null;
        }
        super.close();
    }

    static float[] toFloatArray(ArrayData array, boolean isDouble) {
        if (isDouble) {
            double[] values = array.toDoubleArray();
            float[] out = new float[values.length];
            for (int i = 0; i < values.length; i++) {
                out[i] = (float) values[i];
            }
            return out;
        }
        return array.toFloatArray();
    }
}
