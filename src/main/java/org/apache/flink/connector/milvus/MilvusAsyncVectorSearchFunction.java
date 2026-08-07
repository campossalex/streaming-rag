package org.apache.flink.connector.milvus;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncVectorSearchFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous variant, selected when the VECTOR_SEARCH CONFIG map sets {@code 'async' = 'true'}.
 *
 * <p>The Milvus v2 SDK is blocking, so this issues calls on a bounded pool rather than being
 * natively non-blocking. That still removes the per-record stall from the operator thread and is
 * what makes the difference at realistic throughput, but the pool size is a real concurrency
 * ceiling — size it alongside {@code table.exec.async-vector-search.max-concurrent-operations}.
 */
public class MilvusAsyncVectorSearchFunction extends AsyncVectorSearchFunction {

    private static final long serialVersionUID = 1L;

    private final MilvusConfig config;
    private final MilvusRowConverter converter;
    private final String annsField;
    private final List<String> outputFields;

    private transient MilvusSearchClient client;
    private transient ExecutorService executor;

    public MilvusAsyncVectorSearchFunction(
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
        executor =
                Executors.newFixedThreadPool(
                        config.getAsyncThreadPoolSize(),
                        new ExecutorThreadFactory("milvus-vector-search"));
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncVectorSearch(int topK, RowData queryData) {
        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        if (queryData == null || queryData.isNullAt(0)) {
            future.complete(Collections.emptyList());
            return future;
        }
        final float[] probe =
                MilvusVectorSearchFunction.toFloatArray(
                        queryData.getArray(0), config.isQueryVectorDouble());
        executor.execute(
                () -> {
                    try {
                        List<SearchResp.SearchResult> hits = client.search(probe, topK);
                        List<RowData> rows = new ArrayList<>(hits.size());
                        for (SearchResp.SearchResult hit : hits) {
                            rows.add(converter.toRowData(hit));
                        }
                        future.complete(rows);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
        return future;
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            executor = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
        super.close();
    }
}
