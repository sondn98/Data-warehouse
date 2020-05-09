
package edu.hust.soict.bigdata.facilities.common.wal.impl.csv;

import com.opencsv.CSVWriter;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.WalWriter;
import edu.hust.soict.bigdata.facilities.common.wal.impl.json.JSONWriter;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;

public class CSVWalWriter<M extends DataModel> implements WalWriter<M> {
    private BufferedWriter writer;
    private static final Logger logger = LoggerFactory.getLogger(JSONWriter.class);

    public CSVWalWriter(WalFile file) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(file.absolutePath(), true));
    }

    @Override
    public void append(M data) {
        Field[] fields = data.getClass().getFields();
        String[] record = new String[fields.length];
        for(int i = 0 ; i < fields.length ; i ++){
            try {
                record[i] = String.valueOf(fields[i].get(data));
            } catch (IllegalAccessException e) {
                logger.warn("Can not get value of field " + fields[i].getName() + " in passed data");
                record[i] = "";
            }
        }

        try(CSVWriter csv = new CSVWriter(writer,
                CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)){
            csv.writeNext(record);
            csv.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
