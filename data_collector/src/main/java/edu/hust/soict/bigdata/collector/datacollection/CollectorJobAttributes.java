package edu.hust.soict.bigdata.collector.datacollection;

import edu.hust.soict.bigdata.collector.common.CollectorConst;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.config.Const;

public class CollectorJobAttributes {

    /** Data schema's name. This is also job name */
    public String SCHEMA_NAME;
    /** Number of threads attending to collection */
    public Integer NUM_COLLECTOR_THREADS = Config.getIntProperty(CollectorConst.ACTION_WRITE_EXECUTOR_POOL_SIZE, 10);

    /** Determine if data arrived should be processed in batch*/
    public Boolean ENABLE_BATCH = true;
    /** Folder that files of data arrived should be stored. Only be meaningful if <ENABLE_BATCH> is true*/
    public String LOCAL_WAL_FOLDER;
    /** Time interval between wal scans */
    public Long COLLECT_INTERVAL = Config.getLongProperty(CollectorConst.ACTION_COLLECT_WAL_INTERVAL, 2000L);
    /** Collector periodically scan LOCAL_WAL_FOLDER moves files whose size is greater then [WAL_MAX_SIZE]
     * or expired files (current_time - [WAL_EXPIRATION_TIME] > last_modified_time) to HDFS. This attribute
     * specifies where to put wal file in HDFS. Only be meaningful if <ENABLE_BATCH> is true
     * */
    public String HDFS_WAL_FOLDER;
    /** HDFS connection's name */
    public String HDFS_CONNECTION_NAME = Config.getProperty(Const.HDFS_CLIENT_CONNECTION_NAME);

    /** Determine if data arrived should be treated as streaming flow */
    public Boolean ENABLE_STREAMING = true;
    /** The topic that data arrived should be sent to. Only be meaningful if <ENABLE_STREAMING> is true*/
    public String KAFKA_TOPIC;


    /** Zookeeper connection's name */
    public String ZK_CONNECTION_NAME = Config.getProperty(Const.ZK_CLIENT_CONNECTION_NAME);


}
