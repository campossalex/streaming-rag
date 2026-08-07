package org.apache.flink.connector.milvus;

import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.util.Objects;

/** Serializable snapshot of the connector options, shipped to the task managers. */
public class MilvusConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String token;
    private final String userName;
    private final String password;
    private final String databaseName;
    private final String collectionName;
    private final String metric;
    private final String consistencyLevel;
    private final String filter;
    private final boolean queryVectorIsDouble;
    private final int maxRetries;
    private final long connectTimeoutMs;
    private final int asyncThreadPoolSize;

    public MilvusConfig(ReadableConfig config) {
        String explicitUri = config.getOptional(MilvusOptions.URI).orElse(null);
        if (explicitUri != null) {
            this.uri = explicitUri;
        } else {
            String endpoint =
                    config.getOptional(MilvusOptions.ENDPOINT)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Either 'uri' or 'endpoint' must be set."));
            this.uri = "http://" + endpoint + ":" + config.get(MilvusOptions.PORT);
        }
        this.token = config.getOptional(MilvusOptions.TOKEN).orElse(null);
        this.userName = config.getOptional(MilvusOptions.USER_NAME).orElse(null);
        this.password = config.getOptional(MilvusOptions.PASSWORD).orElse(null);
        this.databaseName = config.get(MilvusOptions.DATABASE_NAME);
        this.collectionName =
                Objects.requireNonNull(
                        config.get(MilvusOptions.COLLECTION_NAME), "'collectionName' is required");
        this.metric = config.get(MilvusOptions.SEARCH_METRIC).toUpperCase();
        this.consistencyLevel = config.get(MilvusOptions.SEARCH_CONSISTENCY_LEVEL).toUpperCase();
        this.filter = config.getOptional(MilvusOptions.SEARCH_FILTER).orElse(null);
        this.queryVectorIsDouble =
                "double".equalsIgnoreCase(config.get(MilvusOptions.QUERY_VECTOR_TYPE));
        this.maxRetries = config.get(MilvusOptions.SEARCH_MAX_RETRIES);
        this.connectTimeoutMs = config.get(MilvusOptions.CONNECT_TIMEOUT).toMillis();
        this.asyncThreadPoolSize = config.get(MilvusOptions.ASYNC_THREAD_POOL_SIZE);
    }

    public String getUri() { return uri; }
    public String getToken() { return token; }
    public String getUserName() { return userName; }
    public String getPassword() { return password; }
    public String getDatabaseName() { return databaseName; }
    public String getCollectionName() { return collectionName; }
    public String getMetric() { return metric; }
    public String getConsistencyLevel() { return consistencyLevel; }
    public String getFilter() { return filter; }
    public boolean isQueryVectorDouble() { return queryVectorIsDouble; }
    public int getMaxRetries() { return maxRetries; }
    public long getConnectTimeoutMs() { return connectTimeoutMs; }
    public int getAsyncThreadPoolSize() { return asyncThreadPoolSize; }

    @Override
    public String toString() {
        return "Milvus[" + uri + "/" + databaseName + "/" + collectionName + ", metric=" + metric + "]";
    }
}
