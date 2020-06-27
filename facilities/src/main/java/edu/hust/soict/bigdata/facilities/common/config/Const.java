package edu.hust.soict.bigdata.facilities.common.config;

public class Const {
    //
    //                          PLATFORMS
    //
    // 1. KAFKA
    public static final String KAFKA_BOOSTRAP_SERVERS = "KAFKA_BOOSTRAP_SERVERS";
    public static final String KAFKA_PRODUCER_TOPIC = "KAFKA_PRODUCER_TOPIC";
    public static final String KAFKA_PRODUCER_ACKS = "KAFKA_PRODUCER_ACKS";
    public static final String KAFKA_PRODUCER_RETRIES = "KAFKA_PRODUCER_RETRIES";
    public static final String KAFKA_PRODUCER_BATCH_SIZE = "KAFKA_PRODUCER_BATCH_SIZE";
    public static final String KAFKA_PRODUCER_LINGER_MS = "KAFKA_PRODUCER_LINGER_MS";
    public static final String KAFKA_PRODUCER_BUFFER_MEMORY = "KAFKA_PRODUCER_BUFFER_MEMORY";
    public static final String KAFKA_PRODUCER_MAX_REQUEST_SIZE = "KAFKA_PRODUCER_MAX_REQUEST_SIZE";
    public static final String KAFKA_PRODUCER_KEY_SERIALIZER = "KAFKA_PRODUCER_KEY_SERIALIZER";
    public static final String KAFKA_PRODUCER_VALUE_SERIALIZER = "KAFKA_PRODUCER_VALUE_SERIALIZER";

    // 2. HDFS
    public static final String HADOOP_FS_URI = "HADOOP_FS_URI";
    public static final String HDFS_DATA_FOLDER = "HDFS_DATA_FOLDER";

    // 3. ZOOKEEPER
    public static final String ZK_HOST = "ZK_HOST";
    public static final String ZK_CLIENT_NAME = "ZK_CLIENT_NAME";
    public static final String ZK_CLIENT_SESSION_TIMEOUT = "ZK_CLIENT_SESSION_TIMEOUT";
    public static final String ZK_INFO_HDFS_NEW_FILE_ZNODE = "ZK_INFO_HDFS_NEW_FILE_ZNODE";

    // 4. HYSTRIX
    public static final String HYSTRIX_METRICS_SERVER_HOST = "HYSTRIX_METRICS_SERVER_HOST";
    public static final String HYSTRIX_METRICS_SERVER_PORT = "HYSTRIX_METRICS_SERVER_PORT";

    // 5. HBASE
    public static final String HBASE_CONFIG_FILE = "HBASE_CONFIG_FILE";
    public static final String HBASE_TABLE = "HBASE_TABLE";
    public static final String HBASE_SCHEMA = "HBASE_SCHEMA";
    public static final String HBASE_COLUMN_FAMILY = "HBASE_COLUMN_FAMILY";
    public static final String HBASE_QUALIFIERS = "HBASE_QUALIFIERS";
    public static final String HBASE_REPOSITORY_CLASS = "HBASE_REPOSITORY_CLASS";
    public static final String SYSTEM_FACILITIES_HBASE_HOME = "SYSTEM_FACILITIES_HBASE_HOME";

    // 6. HIVE
    public static final String HIVE_CONNECTION_URL = "HIVE_CONNECTION_URL";
    public static final String HIVE_TABLE = "HIVE_TABLE";
    public static final String HIVE_SCHEMA = "HIVE_SCHEMA";
    public static final String HIVE_REPOSITORY_CLASS = "HIVE_REPOSITORY_CLASS";

    // 7. ELASTICSEARCH
    public static final String ELASTIC_HOST = "ELASTIC_HOST";
    public static final String ELASTIC_CLUSTER_NAME = "ELASTIC_CLUSTER_NAME";
    public static final String ELASTIC_KAFKA_TOPIC_ON_FAILURE = "ELASTIC_KAFKA_TOPIC_ON_FAILURE";

    //
    //                                COMPONENTS
    //
    // 1. WAL
    public static final String WAL_FILE_TYPE = "WAL_FILE_TYPE";
    public static final String WAL_MAX_SIZE = "WAL_MAX_SIZE";
    public static final String WAL_WRITER_CODEC = "WAL_WRITER_CODEC";
    public static final String WAL_EXPIRATION_TIME = "WAL_EXPIRATION_TIME";
    public static final String LOCAL_FS_WAL_FOLDER = "LOCAL_FS_WAL_FOLDER";

    //
    //                               APPLICATIONS
    //
    // 1. BENCHMARK
    public static final String BENCHMARK_RESTFUL_REQUEST_INTERVAL = "BENCHMARK_RESTFUL_REQUEST_INTERVAL";
    public static final String BENCHMARK_API = "BENCHMARK_API";
    public static final String BENCHMARK_API_TIMEOUT = "BENCHMARK_API_TIMEOUT";
    public static final Integer BENCHMARK_RANDOM_SEED = 5;
    public static final String BENCHMARK_CONTINUOUS = "BENCHMARK_CONTINUOUS";
    public static final String BENCHMARK_MAX_REQUESTS_COUNT = "BENCHMARK_MAX_REQUESTS_COUNT";

    // 2. CHECK FILE STATUS
    public static final String BENCHMARK_CHECK_FILE_INTERVAL = "BENCHMARK_CHECK_FILE_INTERVAL";
    public static final String BENCHMARK_CHECK_FOLDER = "BENCHMARK_CHECK_FOLDER";

}
