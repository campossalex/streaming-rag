package org.apache.flink.connector.milvus;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Thin wrapper over the Milvus v2 SDK. Deliberately free of any Flink types so that it can be
 * unit-tested against a real Milvus without a Flink cluster.
 */
public class MilvusSearchClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MilvusSearchClient.class);

    private final MilvusConfig config;
    private final String annsField;
    private final List<String> outputFields;
    private final IndexParam.MetricType metricType;
    private final ConsistencyLevel consistencyLevel;

    private transient MilvusClientV2 client;

    public MilvusSearchClient(MilvusConfig config, String annsField, List<String> outputFields) {
        this.config = config;
        this.annsField = annsField;
        this.outputFields = outputFields;
        this.metricType = parseMetric(config.getMetric());
        this.consistencyLevel = parseConsistency(config.getConsistencyLevel());
    }

    public void open() {
        ConnectConfig.ConnectConfigBuilder builder =
                ConnectConfig.builder()
                        .uri(config.getUri())
                        .dbName(config.getDatabaseName())
                        .connectTimeoutMs(config.getConnectTimeoutMs());
        if (config.getToken() != null) {
            builder.token(config.getToken());
        } else if (config.getUserName() != null) {
            builder.username(config.getUserName()).password(config.getPassword());
        }
        this.client = new MilvusClientV2(builder.build());
        LOG.info("Opened Milvus client for {} (annsField={})", config, annsField);
    }

    /**
     * Runs a top-k similarity search. Retries transient failures; the collection must already be
     * loaded into memory on the Milvus side or Milvus rejects the search.
     */
    public List<SearchResp.SearchResult> search(float[] queryVector, int topK) throws IOException {
        BaseVector probe = new FloatVec(queryVector);
        // NOTE: the builder is chained end-to-end and never assigned to a named type. SDK 2.5.x
        // declares SearchReqBuilder<C, B> with generics while 3.0.x declares it raw, so naming the
        // type pins the connector to one SDK generation. An empty filter string means "no filter",
        // which lets the optional pre-filter stay inside the chain.
        SearchReq request =
                SearchReq.builder()
                        .collectionName(config.getCollectionName())
                        .annsField(annsField)
                        .data(Collections.singletonList(probe))
                        .topK(topK)
                        .outputFields(outputFields)
                        .metricType(metricType)
                        .consistencyLevel(consistencyLevel)
                        .filter(config.getFilter() == null ? "" : config.getFilter())
                        .build();

        IOException last = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                SearchResp response = client.search(request);
                List<List<SearchResp.SearchResult>> results = response.getSearchResults();
                if (results == null || results.isEmpty()) {
                    return Collections.emptyList();
                }
                // One probe vector in, so exactly one result list out.
                return results.get(0);
            } catch (Exception e) {
                last = new IOException("Milvus search failed on " + config, e);
                LOG.warn("Milvus search attempt {} of {} failed", attempt + 1, config.getMaxRetries() + 1, e);
            }
        }
        throw last;
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Milvus client cleanly", e);
            }
            client = null;
        }
    }

    private static IndexParam.MetricType parseMetric(String metric) {
        try {
            return IndexParam.MetricType.valueOf(metric);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unsupported 'search.metric' value '"
                            + metric
                            + "'. Use COSINE, L2 or IP, and make sure it matches the metric the "
                            + "Milvus index was built with.",
                    e);
        }
    }

    private static ConsistencyLevel parseConsistency(String level) {
        try {
            return ConsistencyLevel.valueOf(level);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unsupported 'search.consistency-level' value '" + level + "'.", e);
        }
    }
}
