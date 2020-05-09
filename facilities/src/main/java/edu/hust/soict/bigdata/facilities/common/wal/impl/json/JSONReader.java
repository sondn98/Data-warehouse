package edu.hust.soict.bigdata.facilities.common.wal.impl.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.facilities.common.exceptions.WalException;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.WalReader;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Dùng để read từ file WAL với codec simple
 *
 * @author <a href="https://github.com/tjeubaoit">tjeubaoit</a>
 */
public class JSONReader<M extends DataModel> implements WalReader<M> {

    private BufferedReader reader;
    private static final ObjectMapper om = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JSONReader.class);

    /**
     * Create new SimpleReader object
     *
     * @param file WAL file
     */
    public JSONReader(WalFile file) {
        try {
            this.reader = new BufferedReader(new FileReader(file.absolutePath()));
            logger.info("Created simple reader on file: " + file.absolutePath());
        } catch (IOException e) {
            throw new WalException(e);
        }
    }

    @Override
    public M next() {
        try {
            String line = reader.readLine();
            return line != null ? om.readValue(line, new TypeReference<M>() {}) : null;
        } catch (IOException e) {
            throw new WalException(e);
        }
    }

    @Override
    public void close() {
        try {
            this.reader.close();
        } catch (IOException e) {
            throw new WalException(e);
        }
    }
}
