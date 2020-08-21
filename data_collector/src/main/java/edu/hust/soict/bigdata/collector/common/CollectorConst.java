package edu.hust.soict.bigdata.collector.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class CollectorConst {

    // 1.COLLECTION
    public static final String COLLECTOR_JOB_ATTR_OBJECT_KEY = "COLLECTOR_JOB_ATTR_OBJECT_KEY";
    public static final String COLLECTOR_SCHEMA_FOLDER = "COLLECTOR_SCHEMA_FOLDER";

    public static final String COLLECTION_JOB_ZK_PARENT = "COLLECTION_JOB_ZK_PARENT";

    // 2. ACTION WRITE
    public static final String ACTION_WRITE_EXECUTOR_POOL_SIZE = "ACTION_WRITE_EXECUTOR_POOL_SIZE";
    public static final String ACTION_WRITE_WAL_TIMEOUT = "ACTION_WRITE_WAL_TIMEOUT";
    public static final String ACTION_WRITE_WAL_ACTIVE = "ACTION_WRITE_WAL_ACTIVE";
    public static final String ACTION_WRITE_KAFKA_TIMEOUT = "ACTION_WRITE_KAFKA_TIMEOUT";
    public static final String ACTION_WRITE_KAFKA_ACTIVE = "ACTION_WRITE_KAFKA_ACTIVE";

    // 3. ACTION COLLECT
    public static final String ACTION_COLLECT_WAL_INTERVAL = "ACTION_COLLECT_WAL_INTERVAL";

    public static final String COLLECTOR_JOB_POOL_SIZE = "COLLECTOR_JOB_POOL_SIZE";

    // 2. RESTFUL
    public static final String LISTENER_SERVER_ADDRESS = "LISTENER_SERVER_ADDRESS";
    public static final String LISTENER_SERVER_PORT = "LISTENER_SERVER_PORT";
    public static final String LISTENER_SERVER_API_CLASSES = "LISTENER_SERVER_API_CLASSES";

    public static final int RESPONSE_CONTINUE = 100;
    public static final int RESPONSE_SWITCHING_PROTOCOL = 101;
    public static final int RESPONSE_PROCESSING = 102;
    public static final int RESPONSE_EARLY_HINTS = 103;

    public static final int RESPONSE_OK = 200;
    public static final int RESPONSE_CREATED = 201;
    public static final int RESPONSE_NON_AUTHORITATIVE_INFORMATION = 203;
    public static final int RESPONSE_NO_CONTENT = 204;

    public static final int RESPONSE_MULTIPLE_CHOICES = 300;
    public static final int RESPONSE_MOVED_PERMANENTLY = 301;
    public static final int RESPONSE_FOUND = 302;
    public static final int RESPONSE_SEE_OTHER = 303;
    public static final int RESPONSE_NOT_MODIFIED = 304;
    public static final int RESPONSE_TEMPORARY_REDIRECTED = 307;

    public static final int RESPONSE_BAD_REQUEST = 400;
    public static final int RESPONSE_UNAUTHORIZED = 401;
    public static final int RESPONSE_FORBIDDEN = 403;
    public static final int RESPONSE_NOT_FOUND = 404;
    public static final int RESPONSE_METHOD_NOT_ALLOWED = 405;
    public static final int RESPONSE_NOT_ACCEPTABLE = 406;
    public static final int RESPONSE_PRECONDITION_FAILED = 412;
    public static final int RESPONSE_UNSUPPORTED_MEDIA_TYPE = 415;

    public static final int RESPONSE_INTERNAL_SERVER_ERROR = 500;
    public static final int RESPONSE_NOT_IMPLEMENTED = 501;

    public static String HOST_NAME;

    static {
        try {
            HOST_NAME = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

}
