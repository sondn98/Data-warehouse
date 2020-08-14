package edu.hust.soict.bigdata.batch;

import edu.hust.soict.bigdata.batch.handler.CSVWalHandler;
import edu.hust.soict.bigdata.batch.handler.Handler;
import edu.hust.soict.bigdata.batch.handler.JSONWalHandler;
import edu.hust.soict.bigdata.batch.handler.ObjectWalHandler;
import edu.hust.soict.bigdata.facilities.common.wal.impl.HdfsFile;
import edu.hust.soict.bigdata.facilities.common.wal.impl.LocalWalFile;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.model.WalInfo;
import edu.hust.soict.bigdata.facilities.platform.hadoop.HdfsConnectionProvider;

class HandlerFactory {

    static <T extends DataModel> Handler<T> getHandler(WalInfo keeper) {
        switch (keeper.codec){
            case "csv":
                return new CSVWalHandler<>(new HdfsFile(HdfsConnectionProvider.getFs(), keeper.filePath, keeper.codec));
            case "object":
                return new ObjectWalHandler<>(new LocalWalFile(keeper.filePath, keeper.codec, keeper.maxSize));
            case "json":
                return new JSONWalHandler<>(new LocalWalFile(keeper.filePath, keeper.codec, keeper.maxSize));
            default:
                throw new UnsupportedOperationException("Codec " + keeper.codec + " isn't supported");
        }
    }
}
