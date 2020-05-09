package edu.hust.soict.bigdata.batch;

import edu.hust.soict.bigdata.batch.handler.CSVWalHandler;
import edu.hust.soict.bigdata.batch.handler.Handler;
import edu.hust.soict.bigdata.batch.handler.JSONWalHandler;
import edu.hust.soict.bigdata.batch.handler.ObjectWalHandler;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.common.wal.impl.HdfsFile;
import edu.hust.soict.bigdata.facilities.common.wal.impl.LocalWalFile;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.platform.hadoop.HdfsReader;

import static edu.hust.soict.bigdata.facilities.common.wal.WalFile.WalInfo;

import java.io.IOException;

class HandlerFactory {

    static <T extends DataModel> Handler<T> getHandler(Properties props, WalInfo keeper) throws IOException {
        switch (keeper.codec){
            case "csv":
                return new CSVWalHandler<>(props, new HdfsFile(HdfsReader.getFs(), keeper.filePath, keeper.codec));
            case "object":
                return new ObjectWalHandler<>(props, new LocalWalFile(keeper.filePath, keeper.codec, keeper.maxSize));
            case "json":
                return new JSONWalHandler<>(props, new LocalWalFile(keeper.filePath, keeper.codec, keeper.maxSize));
            default:
                throw new UnsupportedOperationException("Codec " + keeper.codec + " isn't supported");
        }
    }
}
