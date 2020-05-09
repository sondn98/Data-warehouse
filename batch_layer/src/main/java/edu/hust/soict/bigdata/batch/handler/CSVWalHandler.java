package edu.hust.soict.bigdata.batch.handler;

import edu.hust.soict.bigdata.batch.common.ShellRunner;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

public class CSVWalHandler<M extends DataModel> implements Handler<M> {

    private WalFile wal;
    private Properties props;

    private static final Logger logger = LoggerFactory.getLogger(CSVWalHandler.class);

    public CSVWalHandler(Properties props, WalFile wal){
        this.wal = wal;
        this.props = props;
    }

    @Override
    public void handle() {
        logger.info("Tending to process csv wal file " + wal.name());
        logger.warn("Make sure that hive table was stored by org.apache.hadoop.hive.hbase.HBaseStorageHandler for preventing data losing");
        logger.info("Submitting file...");

        String hbaseHome = props.getProperty(Const.SYSTEM_FACILITIES_HBASE_HOME);
        String importTool = "org.apache.hadoop.hbase.mapreduce.ImportTsv";
        String delimiter = "-Dimporttsv.separator=\",\"";

        String columnFalimy = props.getProperty(Const.HBASE_COLUMN_FAMILY);
        String qulifiers = "HBASE_ROW_KEY," + props.getCollection(Const.HBASE_QUALIFIERS).stream().map(m -> columnFalimy + ":" + m).collect(Collectors.joining(","));
        String columnMapper = "-Dimporttsv.columns=" + qulifiers;

        String table = props.getProperty(Const.HBASE_TABLE);
        String filePath = wal.absolutePath();

        String script = String.join(" ",
                hbaseHome + "/bin/hbase",
                importTool,
                delimiter,
                columnMapper,
                table,
                filePath);
        logger.info(script);

        int stt = ShellRunner.run(script);
        if(stt != 0)
            throw new RuntimeException();

        logger.info("Submitted map-reduce job. Application running");
    }
}
