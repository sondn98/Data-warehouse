package edu.hust.soict.bigdata.batch.handler;

import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.util.Reflects;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.WalReader;
import edu.hust.soict.bigdata.facilities.common.wal.impl.json.JSONReader;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.platform.hbase.HbaseRepository;
import edu.hust.soict.bigdata.facilities.platform.hive.HiveRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JSONWalHandler<M extends DataModel> implements Handler<M> {

    private HbaseRepository<M> hbaseRepository;
    private HiveRepository<M> hiveRepository;

    private WalFile wal;

    private static final ExecutorService service = Executors.newFixedThreadPool(2);
    private static final Logger logger = LoggerFactory.getLogger(JSONWalHandler.class);

    public JSONWalHandler(WalFile wal){
        this.hbaseRepository = Reflects.newInstance(
                Config.getProperty(Const.HBASE_REPOSITORY_CLASS), new Class[]{});
        this.hiveRepository = Reflects.newInstance(
                Config.getProperty(Const.HIVE_REPOSITORY_CLASS), new Class[]{});

        this.wal = wal;
    }

    @Override
    public void handle() {
        WalReader<M> walReader;
        walReader = new JSONReader<>(wal);

        List<M> archived = new LinkedList<>();
        M data;
        while((data = walReader.next()) != null){
            archived.add(data);
        }
        logger.info("Tending to process json wal file " + wal.name());
        service.submit(() -> hbaseRepository.add(archived));
        service.submit(() -> hiveRepository.add(wal.absolutePath()));
    }
}
