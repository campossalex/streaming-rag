package org.apache.flink.connector.milvus;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/**
 * Connector options. The camelCase keys ({@code databaseName}, {@code collectionName},
 * {@code userName}) intentionally mirror the Ververica / Alibaba Milvus connector so that
 * existing VVP DDL can be moved to open-source Flink unchanged.
 */
public final class MilvusOptions {

    private MilvusOptions() {}

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Milvus host name or IP. Combined with 'port' to form the URI.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().defaultValue(19530);

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Full Milvus URI, e.g. https://host:19530. Takes precedence over "
                                    + "'endpoint'/'port'. Use this for TLS or Zilliz Cloud.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("databaseName").stringType().defaultValue("default");

    public static final ConfigOption<String> COLLECTION_NAME =
            ConfigOptions.key("collectionName").stringType().noDefaultValue();

    public static final ConfigOption<String> USER_NAME =
            ConfigOptions.key("userName").stringType().noDefaultValue();

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("API key / token. Alternative to userName + password.");

    public static final ConfigOption<String> SEARCH_METRIC =
            ConfigOptions.key("search.metric")
                    .stringType()
                    .defaultValue("COSINE")
                    .withDescription("One of COSINE, L2, IP. Must match the Milvus index metric.");

    public static final ConfigOption<String> SEARCH_CONSISTENCY_LEVEL =
            ConfigOptions.key("search.consistency-level")
                    .stringType()
                    .defaultValue("BOUNDED")
                    .withDescription("STRONG, BOUNDED, EVENTUALLY or SESSION.");

    public static final ConfigOption<String> SEARCH_FILTER =
            ConfigOptions.key("search.filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional Milvus boolean expression applied as a pre-filter, "
                                    + "e.g. category == \"warranty\".");

    public static final ConfigOption<String> QUERY_VECTOR_TYPE =
            ConfigOptions.key("search.query-vector-type")
                    .stringType()
                    .defaultValue("float")
                    .withDescription(
                            "Element type of the probe vector passed to VECTOR_SEARCH: 'float' for "
                                    + "ARRAY<FLOAT> (what embedding models produce), 'double' for "
                                    + "ARRAY<DOUBLE>. Reading the wrong width silently corrupts the "
                                    + "vector, so this is explicit rather than guessed.");

    public static final ConfigOption<Integer> SEARCH_MAX_RETRIES =
            ConfigOptions.key("search.max-retries").intType().defaultValue(3);

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout").durationType().defaultValue(Duration.ofSeconds(10));

    public static final ConfigOption<Integer> ASYNC_THREAD_POOL_SIZE =
            ConfigOptions.key("async.thread-pool-size")
                    .intType()
                    .defaultValue(8)
                    .withDescription(
                            "Threads per subtask used to issue concurrent Milvus calls when the "
                                    + "VECTOR_SEARCH CONFIG map sets 'async' = 'true'.");
}
