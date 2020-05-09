package edu.hust.soict.bigdata.facilities.common.wal.impl.object;

import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.WalWriter;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ObjectWriter<M extends DataModel> implements WalWriter<M> {

    private static final Logger logger = LoggerFactory.getLogger(ObjectWriter.class);
    private ObjectOutputStream oos;

    public ObjectWriter(WalFile file) throws IOException {
        oos = new ObjectOutputStream(new FileOutputStream(file.absolutePath(), true)){
            @Override
            protected void writeStreamHeader() throws IOException {
                this.enableReplaceObject(false);
                boolean check = true;
                try {
                    ObjectInputStream inStream = new ObjectInputStream(new FileInputStream(file.absolutePath()));
                    if (inStream.readObject() == null) {
                        check = false;
                    }
                    inStream.close();
                } catch (IOException e) {
                    check = false;flush();
                } catch (ClassNotFoundException e) {
                    check = false;
                    e.printStackTrace();
                }

                if(!check) super.writeStreamHeader();
            }
        };
    }

    @Override
    public void append(M model) {
        try{
            oos.writeObject(model);
            oos.flush();
        }catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            oos.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
