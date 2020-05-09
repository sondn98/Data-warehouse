package edu.hust.soict.bigdata.facilities.common.wal.impl.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.facilities.common.exceptions.WalException;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.WalWriter;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Dùng để write vào file WAL với codec simple
 *
 * @author <a href="https://github.com/tjeubaoit">tjeubaoit</a>
 */
public class JSONWriter<M extends DataModel> implements WalWriter<M> {

    private BufferedWriter writer;
    private static final ObjectMapper om = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JSONWriter.class);

    /**
     * Create new SimpleWriter object
     *
     * @param file WAL file
     */
    public JSONWriter(WalFile file) {
        try {
            this.writer = new BufferedWriter(new FileWriter(file.absolutePath(), true));
        } catch (IOException e) {
            throw new WalException(e);
        }
    }

    @Override
    public void append(M data) {
        try {
            this.writer.write(om.writeValueAsString(data) + "\n");
        } catch (IOException e) {
            throw new WalException(e);
        }
    }

    @Override
    public void close() {
        try {
            this.writer.close();
        } catch (IOException e) {
            throw new WalException(e);
        }
    }
}
