package edu.hust.soict.bigdata.collector.action.executors;

import edu.hust.soict.bigdata.facilities.common.wal.WalFactory;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.WalWriter;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WalWriterExecutor<M extends DataModel> implements Runnable {

    private M data;
    private WalFile wal;

    private static final Logger logger = LoggerFactory.getLogger(WalWriterExecutor.class);

    public WalWriterExecutor(WalFile wal, M data) {
        this.data = data;
        this.wal = wal;
    }

    @Override
    public void run() {
        try (WalWriter<M> writer = WalFactory.getWriter(wal)){
            writer.append(data);
        } catch (IOException e) {
//            logger.error(new WalException(e).getMessage());
            e.printStackTrace();
        }
    }
}
