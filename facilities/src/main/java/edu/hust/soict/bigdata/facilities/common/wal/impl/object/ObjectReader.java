package edu.hust.soict.bigdata.facilities.common.wal.impl.object;

import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.WalReader;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class ObjectReader<M extends DataModel> implements WalReader<M> {

    private static final Logger logger = LoggerFactory.getLogger(ObjectReader.class);
    private ObjectInputStream ois;

    public ObjectReader(WalFile file) throws IOException {
        ois = new ObjectInputStream(new FileInputStream(file.absolutePath()));
        logger.info("Created object reader on file: " + file.absolutePath());
    }

    @SuppressWarnings("unchecked")
    @Override
    public M next() {
        try {
            return (M) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }
    }

    @Override
    public void close(){
        try {
            ois.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
